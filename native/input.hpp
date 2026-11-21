#pragma once
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <immintrin.h>
#ifdef _WIN32
#define NOMINMAX
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#endif

using u32 = uint32_t;
using u64 = uint64_t;
using i64 = int64_t;
using SV = std::string_view;
namespace fs = std::filesystem;
constexpr i64 DAY = 86400000, WEEK = DAY * 7, BAD = INT64_MIN;

inline u64 mix(u64 x) {
    x ^= x >> 30; x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27; x *= 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}
template<class F> void parallel(int workers, int jobs, F f) {
    std::atomic<int> next{0};
    auto work = [&](int id) {
        for (;;) {
            int j = next.fetch_add(1, std::memory_order_relaxed);
            if (j >= jobs) break;
            f(id,j);
        }
    };
    std::vector<std::thread> threads;
    for (int i=1;i<std::min(jobs,workers);++i) threads.emplace_back(work,i);
    work(0);
    for(auto& t:threads) t.join();
}
struct Mapping {
    const char* data = nullptr;
    size_t size;
    explicit Mapping(const fs::path& path) : size(fs::file_size(path)) {
        if (!size) return;
#ifdef _WIN32
        auto file = CreateFileW(path.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (file==INVALID_HANDLE_VALUE) throw std::runtime_error("open failed");
        auto map = CreateFileMappingW(file,nullptr,PAGE_READONLY,0,0,nullptr);
        CloseHandle(file);
        if (!map) throw std::runtime_error("mapping failed");
        data = (const char*)MapViewOfFile(map,FILE_MAP_READ,0,0,0);
        CloseHandle(map);
        if (!data) throw std::runtime_error("map view failed");
#else
        int fd=open(path.c_str(),O_RDONLY);
        if(fd<0) throw std::runtime_error("open failed: "+path.string());
        data=(const char*)mmap(nullptr,size,PROT_READ,MAP_PRIVATE,fd,0);
        close(fd);
        if(data==MAP_FAILED) throw std::runtime_error("mmap failed");
#endif
    }
    ~Mapping() {
        if(data)
#ifdef _WIN32
            UnmapViewOfFile(data);
#else
            munmap((void*)data,size);
#endif
    }
};
struct InputFiles {
    std::vector<std::unique_ptr<Mapping>> maps;
    std::vector<SV> parts;
    size_t bytes=0;
    void add(const fs::path& path) {
        auto& m=*maps.emplace_back(std::make_unique<Mapping>(path));
        bytes+=m.size;
        if(!m.size) return;
        SV all(m.data,m.size);
        size_t pos=all.find('\n');
        if(pos==SV::npos) return;
        ++pos;
        while(pos<m.size) {
            size_t end=all.find('\n',std::min(m.size,pos+(2u<<20)));
            end=end==SV::npos?m.size:end+1;
            parts.emplace_back(all.substr(pos,end-pos)); pos=end;
        }
    }
    void folder(const fs::path& p) {
        if(fs::exists(p)) for(auto& e:fs::directory_iterator(p))
            if(e.path().extension()==".csv" && e.is_regular_file()) add(e.path());
    }
};

struct Keys {
    static constexpr u64 extended=1ULL<<63;
    std::mutex lock;
    std::unordered_map<SV,u64> ids;
    std::vector<SV> extra;
    u64 encode(SV s) {
        size_t n=s.size();
        if(n==8 || n==9) {
            u64 a; memcpy(&a,s.data(),8);
            if(!((a|(a-0x0101010101010101ULL))&0x8080808080808080ULL)
                && (n==8 || (unsigned char)(s[8]-1)<127)) {
                u64 k=_pext_u64(__builtin_bswap64(a),0x7f7f7f7f7f7f7f7fULL);
                return k<<7 | (n==9?(unsigned char)s[8]:0);
            }
        } else if(n<=9) {
            u64 k=0; bool ok=true;
            for(unsigned char c:s){if(!c || c>=128){ok=false;break;} k=k<<7|c;}
            if(ok) return k<<((9-n)*7);
        }
        std::lock_guard guard(lock);
        auto [it,inserted]=ids.try_emplace(s,extended|extra.size());
        if(inserted) extra.push_back(s);
        return it->second;
    }
    SV bytes(u64 k,char* storage) const {
        if(k&extended) return extra[k^extended];
        u64 val=__builtin_bswap64(_pdep_u64(k>>7,0x7f7f7f7f7f7f7f7fULL));
        memcpy(storage,&val,8); storage[8]=char(k&127);
        return {storage,k?size_t(9-std::countr_zero(k)/7):0};
    }
    bool less(u64 a,u64 b) const {
        if(!((a|b)&extended)) return a<b;
        char x[9],y[9]; return bytes(a,x)<bytes(b,y);
    }
};
// All input mappings outlive the pipeline, including string views interned above.
template<int N> struct Fields { std::array<SV,N> v; SV operator[](int i)const{return v[i];} };
template<int N> bool scalar_fields(SV row, Fields<N>& f) {
    size_t start=0;
    for(int i=0;i<N;++i) {
        size_t end=row.find(',',start);
        if((end==SV::npos)!=(i==N-1)) return false;
        SV field=row.substr(start,end==SV::npos?end:end-start);
        if(!field.empty() && field.front()=='"') {
            if(field.size()<2 || field.back()!='"') return false;
            field.remove_prefix(1);field.remove_suffix(1);
        }
        if(field.find('"')!=SV::npos) return false;
        f.v[i]=field; start=end+1;
    }
    return true;
}
struct Tokens { u64 lf,commas,quotes; };
inline Tokens scan64(const char* p) {
    auto a=_mm256_loadu_si256((const __m256i*)p), b=_mm256_loadu_si256((const __m256i*)(p+32));
    auto mask=[&](char c)->u64 {
        auto x=_mm256_set1_epi8(c);
        return u32(_mm256_movemask_epi8(_mm256_cmpeq_epi8(a,x))) |
            u64(u32(_mm256_movemask_epi8(_mm256_cmpeq_epi8(b,x))))<<32;
    };
    return {mask('\n'),mask(','),mask('"')};
}
template<int N> inline bool read_row(SV& chunk,Fields<N>& f) {
    auto fallback=[&]() {
        size_t lf=chunk.find('\n');
        SV line=chunk.substr(0,lf);
        chunk.remove_prefix(lf==SV::npos?chunk.size():lf+1);
        return scalar_fields(line,f);
    };
    if(chunk.size()<128) return fallback();
    auto lo=scan64(chunk.data()); Tokens hi{};
    unsigned length;
    if(lo.lf) {
        length=std::countr_zero(lo.lf);
        u64 valid=(lo.lf&-lo.lf)-1;
        lo.commas&=valid;lo.quotes&=valid;
    } else {
        hi=scan64(chunk.data()+64);
        if(!hi.lf) return fallback();
        length=64+std::countr_zero(hi.lf);
        u64 valid=(hi.lf&-hi.lf)-1;
        hi.commas&=valid;hi.quotes&=valid;
    }
    const char* row=chunk.data(); chunk.remove_prefix(length+1);
    int c0=std::popcount(lo.commas);
    if(c0+std::popcount(hi.commas)!=N-1) return false;
    u64 ends0=lo.commas,ends1=hi.commas;
    if(length<64) ends0|=1ULL<<length;
    else ends1|=1ULL<<(length-64);
    u64 starts0=lo.commas<<1|1,starts1=hi.commas<<1|lo.commas>>63;
    u64 open=_pext_u64(lo.quotes,starts0) | (_pext_u64(hi.quotes,starts1)<<std::popcount(starts0));
    u64 close=_pext_u64(lo.quotes<<1,ends0) | (_pext_u64(hi.quotes<<1|lo.quotes>>63,ends1)<<std::popcount(ends0));
    if(open!=close || 2*std::popcount(open)!=std::popcount(lo.quotes)+std::popcount(hi.quotes))return false;
    int start=0;
    for(int i=0;i<N;++i) {
        int end;
        if(i<c0){end=std::countr_zero(lo.commas); lo.commas&=lo.commas-1;}
        else if(i!=N-1){end=64+std::countr_zero(hi.commas); hi.commas&=hi.commas-1;}
        else end=length;
        int q=open&1;open>>=1;
        int size=end-start-2*q;
        if(size<0)return false;
        f.v[i]=SV(row+start+q,size);start=end+1;
    }
    return true;
}
inline i64 number(SV s) {
    if(s.empty())return BAD;
    while(s.size()>1 && s.front()=='0') s.remove_prefix(1);
    i64 v;
    auto [p,err]=std::from_chars(s.data(),s.data()+s.size(),v);
    return (s.front()>='0' && s.front()<='9' && err==std::errc() && p==s.data()+s.size())?v:BAD;
}
struct Dates {
    std::array<unsigned char,65536> pairs;
    std::array<int,120000> months;
    Dates() {
        pairs.fill(255);
        for(int n=0;n<100;++n)pairs[('0'+n/10)|(('0'+n%10)<<8)]=n;
        int days=-719162;
        for(int y=1;y<10000;++y)for(int m=1;m<=12;++m){
            months[y*12+m-1]=days;days+=monthlen(y,m);
        }
    }
    static int monthlen(int y,int m) {
        constexpr int lens[]={31,28,31,30,31,30,31,31,30,31,30,31};
        return lens[m-1]+(m==2 && y%4==0 && (y%100!=0 || y%400==0));
    }
    int two(const char* p)const{uint16_t x;memcpy(&x,p,2);return pairs[x];}
    i64 ingest(SV s)const{
        auto p=s.data();
        int y=two(p)*100+two(p+2);
        return (months[y*12+two(p+5)-1]+two(p+8)-1)*DAY+
            (two(p+11)*3600+two(p+14)*60+two(p+17))*1000LL;
    }
    i64 parse(SV s)const {
        if(s.size()!=20 && s.size()!=25) return number(s);
        const char* p=s.data();
        if(p[4]!='-')return number(s);
        if(p[7]!='-' || p[10]!='T' || p[13]!=':' || p[16]!=':')return BAD;
        int c=two(p), yy=two(p+2), y=c*100+yy, m=two(p+5), d=two(p+8);
        int h=two(p+11),mi=two(p+14),sec=two(p+17);
        if(c>99 || yy>99 || !y || m<1 || m>12 || d<1 || (d>28 && d>monthlen(y,m)) || h>23 || mi>59 || sec>59) return BAD;
        int off=0;
        if(s.size()==20){if(p[19]!='Z')return BAD;}
        else {
            if((p[19]!='+' && p[19]!='-') || p[22]!=':')return BAD;
            int oh=two(p+20),om=two(p+23);if(oh>23||om>59)return BAD;
            off=(oh*60+om)*(p[19]=='+'?1:-1);
        }
        return (months[y*12+m-1]+d-1)*DAY+((h*60+mi-off)*60+sec)*1000LL;
    }
};
