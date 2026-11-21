#include "input.hpp"
#include "parquet.hpp"
#include <cstdlib>
#include <iostream>
#include <iterator>

constexpr int PARTS=64;
inline int part(u64 hash){return int(hash&(PARTS-1));}
inline int epochDay(i64 t){return int(t>=0?t/DAY:(t+1)/DAY-1);}

// The hash slot is a stable numeric user ID. Insertion probes are bounded;
// unusually long collision chains use an exact-key overflow table.
class People {
    struct Slot {u64 key=0;u32 parent=0;};
    std::vector<Slot> slots,extra;
    std::unordered_map<u64,u32> extraIds;
    std::mutex mutex;
    const Keys& keys;
    u32 mask;
    Slot& entry(u32 id){return id<=slots.size()?slots[id-1]:extra[id-slots.size()-1];}
    u32 root(u32 id){
        u32 r=id;
        while(entry(r).parent)r=entry(r).parent;
        while(id!=r){auto& e=entry(id);u32 next=e.parent;e.parent=r;id=next;}
        return r;
    }
public:
    People(size_t estimate,const Keys& k):slots(std::bit_ceil(std::clamp<size_t>(estimate,256,1u<<25))),keys(k),mask(slots.size()-1){}
    void prefetch(u64 hash)const {__builtin_prefetch(&slots[hash&mask],0,1);}
    template<bool resolve> u32 lookup(u64 key,u64 hash){
        unsigned i=hash&mask;
        for(unsigned n=0;n<12;++n,i=(i+1)&mask){
            auto& slot=slots[i];std::atomic_ref<u64> ref(slot.key);
            u64 found=ref.load(std::memory_order_relaxed);
            if(!found && ref.compare_exchange_strong(found,key,std::memory_order_relaxed))found=key;
            if(found==key)return resolve && slot.parent?slot.parent:i+1;
        }
        std::lock_guard guard(mutex);
        auto [it,added]=extraIds.try_emplace(key,slots.size()+extra.size()+1);
        if(added)extra.push_back({key,0});
        u32 id=it->second;
        return resolve && entry(id).parent?entry(id).parent:id;
    }
    void link(u64 a,u64 b){
        u32 x=root(lookup<false>(a,mix(a))), y=root(lookup<false>(b,mix(b)));
        if(x!=y){
            if(keys.less(entry(y).key,entry(x).key))std::swap(x,y);
            entry(y).parent=x;
        }
    }
    void flatten(){
        for(u32 i=0;i<slots.size();++i)if(slots[i].parent)root(i+1);
        for(u32 i=0;i<extra.size();++i)root(slots.size()+1+i);
    }
    u64 name(u32 id){return entry(id).key;}
    size_t groups()const{return (slots.size()+extra.size())/PARTS+1;}
};
struct SmallValue {
    u64 data;
    static SmallValue make(i64 time,i64 start,int category,int price){
        return {u64(time-start)<<32 | u32(category)<<24 | u32(price)};
    }
    i64 time(i64 start)const{return start+i64(data>>32);}
    u32 category()const{return u32(data)>>24;}
    u32 price()const{return u32(data)&0xffffff;}
    bool operator<(SmallValue rhs)const{return data<rhs.data;}
};
struct WideValue {
    i64 ts;u32 cat,amount;
    static WideValue make(i64 time,i64,int category,int price){return {time,u32(category),u32(price)};}
    i64 time(i64)const{return ts;}
    u32 category()const{return cat;}
    u32 price()const{return amount;}
    bool operator<(WideValue rhs)const{return ts<rhs.ts;}
};
template<class V> struct Event{V value;u32 person;};
struct Ad{u64 id;i64 ts;u32 campaign,person;};
struct Candidate{u64 id;u32 seconds,target;};
struct Overflow{i64 seconds;int shard,index;};
struct Stats{i64 sessions=0,purchases=0,revenue=0,assists=0;};
template<class V> struct Buffers {
    std::array<std::vector<Event<V>>,PARTS> events;
    std::array<std::vector<Ad>,PARTS> clicks;
    std::array<std::vector<Candidate>,PARTS> candidates;
    std::array<std::unordered_map<u32,Overflow>,PARTS> wide;
};
struct Winner{u64 id=0;u32 worker,index;};

struct Pipeline {
    fs::path in,out;int cpus;
    Keys keys;Dates dates; i64 start,end;
    InputFiles products,campaigns,links,events,clicks;
    std::unordered_map<u64,u32> productCategory,campaignIndex;
    std::vector<u64> cats,adIds,channels;
    std::unique_ptr<People> directory;
    Pipeline(fs::path input,fs::path output,int threads):in(input),out(output),cpus(threads){
        std::ifstream file(in/"manifest.json");
        std::string json{std::istreambuf_iterator<char>(file),{}};
        auto stamp=[&](SV name){
            size_t a=json.find(name);a=json.find(':',a);a=json.find('"',a);
            size_t b=json.find('"',a+1);return dates.parse(SV(json).substr(a+1,b-a-1));
        };
        start=stamp("window_start");end=stamp("window_end");
        if(start==BAD||end==BAD||end<=start)throw std::runtime_error("invalid manifest window");
        products.add(in/"products.csv");campaigns.add(in/"campaigns.csv");
        links.folder(in/"links");events.folder(in/"events");clicks.folder(in/"clicks");
        cats.push_back(keys.encode("unknown"));
        std::unordered_map<u64,u32> categoryIds{{cats[0],0}};
        pairs(products,[&](u64 product,u64 cat){auto [it,fresh]=categoryIds.try_emplace(cat,cats.size());
            if(fresh)cats.push_back(cat);
            productCategory.emplace(product,it->second);
        });
        adIds.push_back(keys.encode("organic"));channels.push_back(adIds[0]);
        pairs(campaigns,[&](u64 id,u64 channel){campaignIndex.emplace(id,adIds.size());adIds.push_back(id);channels.push_back(channel);});
        directory=std::make_unique<People>(std::max(links.bytes/5,events.bytes/600),keys);
        pairs(links,[&](u64 a,u64 b){if(a && b && a!=b)directory->link(a,b);});
        directory->flatten();
        fs::create_directories(out/"user_daily");fs::create_directories(out/"campaign_daily");
    }
    template<class F> void pairs(const InputFiles& files,F f){
        Fields<2> fields;for(auto chunk:files.parts)while(!chunk.empty())if(read_row(chunk,fields))f(keys.encode(fields[0]),keys.encode(fields[1]));
    }
    template<class V> void parseEvents(SV chunk,Buffers<V>& buf){
        struct Row{u64 id,key,hash;i64 ts,ingest;int price,cat;};
        std::array<Row,32> batch;int size=0;Fields<7> fields;
        auto flush=[&]{
            for(int j=0;j<size;++j){
                const auto& r=batch[j];int shard=0,index=-1;
                if(r.key){
                    u32 person=directory->lookup<true>(r.key,r.hash);shard=part(person);
                    auto& dest=buf.events[shard];index=dest.size();
                    dest.push_back({V::make(r.ts,start,r.cat,r.price),person});
                }
                int s=part(mix(r.id));auto& ids=buf.candidates[s];
                i64 seconds=r.ingest/1000-start/1000+INT32_MAX;
                if(u64(seconds)<UINT32_MAX && index<0x3ffffff){
                    ids.push_back({r.id,u32(seconds),index<0?UINT32_MAX:(u32(index)<<6)|u32(shard)});
                }else{
                    buf.wide[s][ids.size()]={seconds,shard,index};
                    ids.push_back({r.id,UINT32_MAX,0});
                }
            }
            size=0;
        };
        while(!chunk.empty()){
            if(!read_row(chunk,fields) || fields[0].empty())continue;
            Row r{};r.id=keys.encode(fields[0]);r.ts=dates.parse(fields[2]);r.ingest=dates.ingest(fields[3]);
            if(r.ts>=start && r.ts<end && r.ingest-r.ts<=DAY && !fields[1].empty() && !fields[5].empty()){
                bool valid=fields[4]=="view" || fields[4]=="cart";
                if(fields[4]=="purchase"){
                    i64 amount=number(fields[6]);
                    if(amount>=1 && amount<=10000000){
                        valid=true;r.price=amount;auto it=productCategory.find(keys.encode(fields[5]));
                        r.cat=it==productCategory.end()?0:it->second;
                    }
                }
                if(valid){r.key=keys.encode(fields[1]);r.hash=mix(r.key);directory->prefetch(r.hash);}
            }
            batch[size++]=r;if(size==int(batch.size()))flush();
        }
        flush();
    }
    template<class V> void parseClicks(SV chunk,Buffers<V>& buf){
        struct Pending{u64 key,hash,id;i64 ts;u32 campaign;};
        std::array<Pending,32> batch;int size=0;Fields<4> fields;
        auto flush=[&]{
            for(int j=0;j<size;++j){
                auto r=batch[j];u32 person=directory->lookup<true>(r.key,r.hash);
                buf.clicks[part(person)].push_back({r.id,r.ts,r.campaign,person});
            }
            size=0;
        };
        while(!chunk.empty()){
            if(!read_row(chunk,fields)||fields[0].empty()||fields[1].empty())continue;
            i64 ts=dates.parse(fields[3]);if(ts<start-WEEK||ts>=end)continue;
            auto it=campaignIndex.find(keys.encode(fields[2]));if(it==campaignIndex.end())continue;
            u64 key=keys.encode(fields[1]),h=mix(key);directory->prefetch(h);
            batch[size++]={key,h,keys.encode(fields[0]),ts,it->second};
            if(size==int(batch.size()))flush();
        }
        flush();
    }
    template<class V> void dedup(std::vector<Buffers<V>>& workers,int shard){
        size_t n=0;for(auto& w:workers)n+=w.candidates[shard].size();if(!n)return;
        size_t bits=std::bit_ceil(std::max<size_t>(64,n*12)), collisions=0;
        std::vector<u64> seen(bits/64),repeated(bits/64);
        for(auto& w:workers)for(const auto& r:w.candidates[shard]){
            size_t bit=(mix(r.id)>>6)&(bits-1);u64 flag=1ULL<<(bit%64);
            if(seen[bit/64]&flag){collisions+=1+!(repeated[bit/64]&flag);repeated[bit/64]|=flag;}
            seen[bit/64]|=flag;
        }
        std::vector<Winner> winners(std::bit_ceil(collisions+collisions/2+1));
        auto info=[&](int w,int i,const Candidate& r)->Overflow{
            if(r.seconds==UINT32_MAX)return workers[w].wide[shard].at(i);
            return {r.seconds,int(r.target%PARTS),r.target==UINT32_MAX?-1:int(r.target>>6)};
        };
        for(u32 w=0;w<workers.size();++w){
            auto& ids=workers[w].candidates[shard];
            for(u32 i=0;i<ids.size();++i){
                auto& r=ids[i];u64 h=mix(r.id)>>6;size_t bit=h&(bits-1);
                if(!(repeated[bit/64]&(1ULL<<(bit%64))))continue;
                size_t slot=(h^(h>>19))&(winners.size()-1);
                while(winners[slot].id && winners[slot].id!=r.id)slot=(slot+1)&(winners.size()-1);
                auto& current=winners[slot];
                if(current.id){
                    auto previous=info(current.worker,current.index,workers[current.worker].candidates[shard][current.index]);
                    auto next=info(w,i,r);
                    if(previous.seconds<=next.seconds){
                        if(next.index>=0)workers[w].events[next.shard][next.index].person=0;
                        continue;
                    }
                    if(previous.index>=0)workers[current.worker].events[previous.shard][previous.index].person=0;
                }
                current={r.id,w,i};
            }
        }
        for(auto& w:workers)std::vector<Candidate>().swap(w.candidates[shard]);
    }
    template<class V> void process(std::vector<Buffers<V>>& workers,int shard,std::vector<CampaignDay>& result){
        struct Counts{u32 e=0,c=0;};std::vector<Counts> offsets(directory->groups());
        for(auto& w:workers){
            for(auto& row:w.events[shard])if(row.person)offsets[row.person>>6].e++;
            for(auto& row:w.clicks[shard])offsets[row.person>>6].c++;
        }
        Counts total;for(auto& pos:offsets){auto count=pos;pos=total;total.e+=count.e;total.c+=count.c;}
        auto ev=std::make_unique_for_overwrite<V[]>(total.e);
        auto ads=std::make_unique_for_overwrite<Ad[]>(total.c);
        for(auto& w:workers){
            for(auto& r:w.events[shard])if(r.person)ev[offsets[r.person>>6].e++]=r.value;
            for(auto& r:w.clicks[shard])ads[offsets[r.person>>6].c++]=r;
            std::vector<Event<V>>().swap(w.events[shard]);std::vector<Ad>().swap(w.clicks[shard]);
        }
        std::vector<UserDay> users;std::unordered_map<int,std::vector<Stats>> byDay;
        std::vector<i64> revenue(cats.size());std::vector<u32> touched;
        std::vector<u32> active,at(adIds.size()),freq(adIds.size());
        u32 i=0,clickEnd=0;
        for(u32 g=0;g<offsets.size();++g){
            u32 limit=offsets[g].e,first=clickEnd,next=clickEnd;clickEnd=offsets[g].c;
            if(i==limit)continue;
            std::sort(ev.get()+i,ev.get()+limit);
            std::sort(ads.get()+first,ads.get()+clickEnd,[&](const Ad& a,const Ad& b){return a.ts!=b.ts?a.ts<b.ts:keys.less(b.id,a.id);});
            for(auto c:active)freq[c]=0;active.clear();
            u64 name=directory->name((g<<6)|shard);
            while(i<limit){
                auto day=epochDay(ev[i].time(start));UserDay user{name,0,day,0,0,0};
                auto& daily=byDay[day];if(daily.empty())daily.resize(adIds.size());
                do{
                    i64 session=ev[i].time(start),last=session,amount=0;
                    while(first<next && ads[first].ts<session-WEEK){
                        u32 c=ads[first++].campaign;
                        if(--freq[c]==0){u32 moved=active.back();active[at[c]]=moved;at[moved]=at[c];active.pop_back();}
                    }
                    while(next<clickEnd && ads[next].ts<=session){
                        const auto& ad=ads[next++];
                        if(ad.ts<session-WEEK){first=next;continue;}
                        if(freq[ad.campaign]++==0){at[ad.campaign]=active.size();active.push_back(ad.campaign);}
                    }
                    u32 from=i;
                    do{
                        auto value=ev[i++];last=value.time(start);u32 price=value.price();
                        if(price){u32 c=value.category();if(!revenue[c])touched.push_back(c);revenue[c]+=price;amount+=price;}
                    }while(i<limit && ev[i].time(start)-last<=1800000);
                    auto& credit=daily[next>first?ads[next-1].campaign:0];
                    credit.sessions++;credit.revenue+=amount;credit.purchases+=amount!=0;
                    for(u32 c:active)daily[c].assists++;
                    user.sessions++;user.events+=i-from;user.revenue+=amount;
                }while(i<limit && epochDay(ev[i].time(start))==day);
                i64 best=0;for(auto c:touched){auto sum=revenue[c];if(sum>best || (sum==best&&keys.less(cats[c],user.category))){best=sum;user.category=cats[c];}revenue[c]=0;}
                touched.clear();users.push_back(user);
            }
        }
        if(shard==0 || !users.empty())parquet(out/"user_daily"/("part-"+std::to_string(shard)+".parquet"),keys,users,usersSchema);
        for(auto& [day,values]:byDay)for(u32 c=0;c<values.size();++c){
            auto& s=values[c];if(s.sessions||s.assists)result.push_back({adIds[c],channels[c],day,s.sessions,s.purchases,s.revenue,s.assists});
        }
    }
    template<class V> void run(){
        std::vector<Buffers<V>> workers(cpus);
        for(auto& w:workers)for(int s=0;s<PARTS;s++){
            size_t n=events.bytes/(82*size_t(cpus)*PARTS);
            w.events[s].reserve(n);w.candidates[s].reserve(n);
            w.clicks[s].reserve(clicks.bytes/(48*size_t(cpus)*PARTS));
        }
        parallel(cpus,events.parts.size()+clicks.parts.size(),[&](int w,int j){
            if(size_t(j)<events.parts.size())parseEvents(events.parts[j],workers[w]);
            else parseClicks(clicks.parts[j-events.parts.size()],workers[w]);
        });
        parallel(cpus,PARTS,[&](int,int s){dedup(workers,s);});
        std::array<std::vector<CampaignDay>,PARTS> partial;
        parallel(cpus,PARTS,[&](int,int s){process(workers,s,partial[s]);});
        std::vector<CampaignDay> rows;for(auto& p:partial)rows.insert(rows.end(),p.begin(),p.end());
        auto less=[](const CampaignDay& a,const CampaignDay& b){return std::tie(a.campaign,a.day)<std::tie(b.campaign,b.day);};
        std::sort(rows.begin(),rows.end(),less);
        size_t n=0;for(auto r:rows){if(n && rows[n-1].campaign==r.campaign && rows[n-1].day==r.day){
            auto& a=rows[n-1];a.sessions+=r.sessions;a.purchases+=r.purchases;a.revenue+=r.revenue;a.assists+=r.assists;
        }else rows[n++]=r;}
        rows.resize(n);parquet(out/"campaign_daily"/"part-0.parquet",keys,rows,adsSchema);
    }
};
int main(int argc,char** argv){
    try{
        auto env=[](const char* k,const char* fallback){const char* v=getenv(k);return v?v:fallback;};
        Pipeline p(argc>1?argv[1]:env("DATA_DIR","/data"),argc>2?argv[2]:env("OUTPUT_DIR","/output"),std::max(1,atoi(env("NCPU","8"))));
        if(p.end-p.start<=i64(1ULL<<32)&&p.cats.size()<=256)p.run<SmallValue>();else p.run<WideValue>();
    } catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}
}
