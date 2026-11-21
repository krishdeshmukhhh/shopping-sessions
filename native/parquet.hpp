#pragma once
#include "input.hpp"

using Bytes=std::vector<unsigned char>;
inline void append(Bytes& b,const void* p,size_t n) {
    const auto* s=static_cast<const unsigned char*>(p);
    b.insert(b.end(),s,s+n);
}
template<class T> void le(Bytes& b,T v){append(b,&v,sizeof(v));}
inline void varint(Bytes& b,u64 x){
    while(x>=128){b.push_back((x&127)|128);x>>=7;} b.push_back(x);
}
inline void zig(Bytes& b,i64 x){varint(b,(u64(x)<<1)^u64(x>>63));}
inline void binary(Bytes& b,SV s){varint(b,s.size());append(b,s.data(),s.size());}
struct Compact {
    Bytes& b; int last=0;
    void field(int id,int type){b.push_back(((id-last)<<4)|type);last=id;}
    void integer(int id,i64 x){field(id,5);zig(b,x);}
    void large(int id,i64 x){field(id,6);zig(b,x);}
    void text(int id,SV s){field(id,8);binary(b,s);}
    void list(int id,int type,int n){field(id,9);b.push_back(std::min(n,15)<<4|type);if(n>=15)varint(b,n);}
    Compact sub(int id){field(id,12);return {b};}
    void end(){b.push_back(0);}
};
struct Column {const char* name;int type,converted;bool optional=false;};
struct UserDay {u64 person,category;int day; i64 sessions,events,revenue;};
struct CampaignDay {u64 campaign,channel;int day;i64 sessions,purchases,revenue,assists;};
constexpr Column usersSchema[]={{"user_id",6,0},{"day",1,6},{"sessions",2,-1},{"events",2,-1},{"revenue_cents",2,-1},{"top_category",6,0,true}};
constexpr Column adsSchema[]={{"campaign_id",6,0},{"channel",6,0},{"day",1,6},{"sessions",2,-1},{"purchasing_sessions",2,-1},{"revenue_cents",2,-1},{"assisted_sessions",2,-1}};
inline void stringValue(Bytes& b,const Keys& keys,u64 key){
    char buf[9];SV s=keys.bytes(key,buf);le(b,u32(s.size()));append(b,s.data(),s.size());
}
inline void encode(Bytes& b,const Keys& keys,std::span<const UserDay> rows,int c){
    if(c==5){
        size_t groups=(rows.size()+7)/8;
        Bytes run;varint(run,groups*2+1);le(b,u32(groups+run.size()));append(b,run.data(),run.size());
        for(size_t i=0;i<rows.size();i+=8){
            unsigned mask=0;
            for(size_t j=0;j<8 && i+j<rows.size();++j)if(rows[i+j].revenue)mask|=1u<<j;
            b.push_back(mask);
        }
    }
    switch(c){
        case 0:for(auto& r:rows)stringValue(b,keys,r.person);break;
        case 1:for(auto& r:rows)le(b,int32_t(r.day));break;
        case 2:for(auto& r:rows)le(b,r.sessions);break;
        case 3:for(auto& r:rows)le(b,r.events);break;
        case 4:for(auto& r:rows)le(b,r.revenue);break;
        case 5:for(auto& r:rows)if(r.revenue)stringValue(b,keys,r.category);break;
    }
}
inline void encode(Bytes& b,const Keys& keys,std::span<const CampaignDay> rows,int c){
    switch(c){
        case 0:for(auto& r:rows)stringValue(b,keys,r.campaign);break;
        case 1:for(auto& r:rows)stringValue(b,keys,r.channel);break;
        case 2:for(auto& r:rows)le(b,int32_t(r.day));break;
        case 3:for(auto& r:rows)le(b,r.sessions);break;
        case 4:for(auto& r:rows)le(b,r.purchases);break;
        case 5:for(auto& r:rows)le(b,r.revenue);break;
        case 6:for(auto& r:rows)le(b,r.assists);break;
    }
}
template<class T,size_t N> void parquet(const fs::path& path,const Keys& keys,const std::vector<T>& rows,const Column(&schema)[N]){
    struct Chunk{i64 pos,size;};
    struct Group{int count;std::array<Chunk,N> chunks;};
    std::vector<Group> groups;
    std::ofstream out(path,std::ios::binary);out.exceptions(std::ios::badbit|std::ios::failbit);
    out.write("PAR1",4);i64 offset=4;Bytes page,header;
    for(size_t start=0;start<rows.size();start+=32768){
        auto block=std::span<const T>(rows).subspan(start,std::min<size_t>(32768,rows.size()-start));
        auto& g=groups.emplace_back();g.count=int(block.size());
        for(size_t i=0;i<N;++i){
            page.clear();header.clear();encode(page,keys,block,i);
            Compact h{header};h.integer(1,0);h.integer(2,page.size());h.integer(3,page.size());
            auto d=h.sub(5);d.integer(1,g.count);d.integer(2,0);d.integer(3,3);d.integer(4,3);d.end();h.end();
            i64 bytes=header.size()+page.size();g.chunks[i]={offset,bytes};offset+=bytes;
            out.write((const char*)header.data(),header.size());out.write((const char*)page.data(),page.size());
        }
    }
    Bytes footer;Compact f{footer};f.integer(1,1);f.list(2,12,N+1);
    Compact root{footer};root.text(4,"schema");root.integer(5,N);root.end();
    for(auto& col:schema){
        Compact c{footer};c.integer(1,col.type);c.integer(3,col.optional);c.text(4,col.name);
        if(col.converted>=0)c.integer(6,col.converted);c.end();
    }
    f.large(3,rows.size());f.list(4,12,groups.size());
    for(auto& g:groups){
        Compact group{footer};group.list(1,12,N);i64 size=0;
        for(size_t i=0;i<N;++i){
            auto& col=schema[i];auto& chunk=g.chunks[i];size+=chunk.size;
            Compact c{footer};c.large(2,0);auto meta=c.sub(3);meta.integer(1,col.type);
            meta.list(2,5,2);zig(footer,0);zig(footer,3);meta.list(3,8,1);binary(footer,col.name);
            meta.integer(4,0);meta.large(5,g.count);meta.large(6,chunk.size);meta.large(7,chunk.size);meta.large(9,chunk.pos);
            meta.end();c.end();
        }
        group.large(2,size);group.large(3,g.count);group.end();
    }
    f.end();u32 size=footer.size();
    out.write((const char*)footer.data(),size);out.write((const char*)&size,4);out.write("PAR1",4);
}
