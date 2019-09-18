#include <iostream>
#include <boost/python.hpp>
#include <boost/python/numpy.hpp>
#include <map>
#include <unordered_map>
#include <ctime>
#include <vector>
#include <cstring>
#include <cstdio>
#include <algorithm>
using namespace std;
using namespace boost::python;
namespace np = boost::python::numpy;

np::ndarray convertToHex(np::ndarray &a) {
    clock_t all_st = clock();
    int len = a.shape(0);

    clock_t st,ed;

    st = clock();
    boost::python::str *arr = reinterpret_cast<boost::python::str*>(a.get_data());
    ed = clock();
    //std::cout << "get data: " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    st = clock();
    vector<unsigned int> s;
    s.resize(len);
    for (int i = 0; i < len; i++) {
        string tmp = boost::python::extract<std::string>(arr[i]);
        unsigned int now;
        if (tmp == "") {
            now = 0;
        } else {
            now = std::stoul(tmp, nullptr, 16);
        }
        s[i] = now;
    }
    ed = clock();
    //std::cout << "init vector: " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    st = clock();
    Py_intptr_t shape[1] = {len};
    np::ndarray result = np::zeros(1,shape, np::dtype::get_builtin<int>());
    std::copy(s.begin(), s.end(), reinterpret_cast<int*>(result.get_data()));
    ed = clock();
    //std::cout << "compute result : " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    /*
    vector<int> pr;
    pr.resize(len);

    for (int i = 0; i < len; i++) {
        if (p[i].first == "nan" || p[i].second == "nan") {
            pr[i] = 0;
        } else {
            pr[i] = m[p[i]];
        }
    }
    */
    clock_t all_ed = clock();
    //std::cout << "all in c : " << 1.0 * (all_ed - all_st) / CLOCKS_PER_SEC <<std::endl;
    return result;

}

np::ndarray compute_by_sort_no_merge(np::ndarray &a) {
    clock_t all_st = clock();
    int len = a.shape(0);

    clock_t st,ed;

    st = clock();
    boost::python::str *arr = reinterpret_cast<boost::python::str*>(a.get_data());
    ed = clock();
    std::cout << "get data: " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    st = clock();
    vector<pair<long long, int> > s;
    vector<int> flags;
    flags.resize(len);
    long long p = 1ll << 32;
    s.resize(len);
    for (int i = 0; i < len; i++) {
        string tmp = boost::python::extract<std::string>(arr[i]);
        unsigned int now;
        flags[i] = 1;
        if (tmp == "") {
            flags[i] = 0;
            now = -1;
        } else {
            now = std::stoul(tmp, nullptr, 16);
        }
        tmp = boost::python::extract<std::string>(arr[i + len]);
        unsigned int now2;
        if (tmp == "") {
            flags[i] = 0;
            now2 = -1;
        } else {
            now2 = std::stoul(tmp, nullptr, 16);
        }
        s[i].first = 1ll * now * p + now2;
        s[i].second = i;
    }
    ed = clock();
    std::cout << "init vector: " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    st = clock();
    sort(s.begin(), s.end());
    ed = clock();
    std::cout << "sort: " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    st = clock();
    Py_intptr_t shape[1] = {len};
    np::ndarray result = np::zeros(1,shape, np::dtype::get_builtin<int>());
    vector<int> result_vt;
    result_vt.resize(len);
    int cnt = 0;
    int last = 0;
    for (int i = 0; i < len; i++) {
        if (i == 0 || s[i].first != s[i - 1].first) {
            for (int j = last; j < i; j++) {
                result_vt[s[j].second] = cnt;
                if (flags[s[j].second] == 0) result_vt[s[j].second] = 0;
            }
            cnt =  1;
            last = i;
        } else cnt++;
    }

    for (int i = last; i < len; i++) {
        result_vt[s[i].second] = cnt;
        if (flags[s[i].second] == 0) result_vt[s[i].second] = 0;
    }

    std::copy(result_vt.begin(), result_vt.end(), reinterpret_cast<int*>(result.get_data()));
    ed = clock();
    std::cout << "compute result : " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    /*
    vector<int> pr;
    pr.resize(len);

    for (int i = 0; i < len; i++) {
        if (p[i].first == "nan" || p[i].second == "nan") {
            pr[i] = 0;
        } else {
            pr[i] = m[p[i]];
        }
    }
    */
    clock_t all_ed = clock();
    std::cout << "all in c : " << 1.0 * (all_ed - all_st) / CLOCKS_PER_SEC <<std::endl;
    return result;

}

np::ndarray compute_by_map_no_merge(np::ndarray &a) {
    clock_t all_st = clock();
    int len = a.shape(0);

    clock_t st,ed;

    st = clock();
    boost::python::str *arr = reinterpret_cast<boost::python::str*>(a.get_data());
    ed = clock();
    std::cout << "get data: " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    st = clock();
    vector<long long> s;
    long long p = 1ll << 32;
    unordered_map<long long, int> m;
    s.resize(len);
    for (int i = 0; i < len; i++) {
        string tmp = boost::python::extract<std::string>(arr[i]);
        int now;
        if (tmp == "") {
            now = -1;
        } else 
            now = std::stoul(tmp, nullptr, 16);
        tmp = boost::python::extract<std::string>(arr[i + len]);
        int now2 = std::stoul(tmp, nullptr, 16);
        s[i] = now * p + now2;
        m[s[i]]++;
    }
    ed = clock();
    std::cout << "init map: " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    st = clock();
    Py_intptr_t shape[1] = {len};
    np::ndarray result = np::zeros(1,shape, np::dtype::get_builtin<int>());
    vector<int> result_vt;
    result_vt.resize(len);
    for (int i = 0; i < len; i++) {
        result_vt[i] = m[s[i]];
    }

    std::copy(result_vt.begin(), result_vt.end(), reinterpret_cast<int*>(result.get_data()));
    ed = clock();
    std::cout << "compute result : " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    /*
    vector<int> pr;
    pr.resize(len);

    for (int i = 0; i < len; i++) {
        if (p[i].first == "nan" || p[i].second == "nan") {
            pr[i] = 0;
        } else {
            pr[i] = m[p[i]];
        }
    }
    */
    clock_t all_ed = clock();
    std::cout << "all in c : " << 1.0 * (all_ed - all_st) / CLOCKS_PER_SEC <<std::endl;
    return result;

}

np::ndarray compute_by_map(np::ndarray &a) {
    clock_t all_st = clock();
    int len = a.shape(0);

    clock_t st,ed;

    st = clock();
    boost::python::str *arr = reinterpret_cast<boost::python::str*>(a.get_data());
    ed = clock();
    std::cout << "get data: " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    st = clock();
    vector<string> s;
    unordered_map<string, int> m;
    s.resize(len);
    for (int i = 0; i < len; i++) {
        s[i] = boost::python::extract<std::string>(arr[i]);
        m[s[i]]++;
    }
    ed = clock();
    std::cout << "init map: " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    st = clock();
    Py_intptr_t shape[1] = {len};
    np::ndarray result = np::zeros(1,shape, np::dtype::get_builtin<int>());
    vector<int> result_vt;
    result_vt.resize(len);
    int cnt = 0;
    int last = 0;
    for (int i = 0; i < len; i++) {
        result_vt[i] = m[s[i]];
    }

    std::copy(result_vt.begin(), result_vt.end(), reinterpret_cast<int*>(result.get_data()));
    ed = clock();
    std::cout << "compute result : " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    /*
    vector<int> pr;
    pr.resize(len);

    for (int i = 0; i < len; i++) {
        if (p[i].first == "nan" || p[i].second == "nan") {
            pr[i] = 0;
        } else {
            pr[i] = m[p[i]];
        }
    }
    */
    clock_t all_ed = clock();
    std::cout << "all in c : " << 1.0 * (all_ed - all_st) / CLOCKS_PER_SEC <<std::endl;
    return result;

}

np::ndarray compute_by_sort_in_boost(np::ndarray &a) {
    clock_t all_st = clock();
    int len = a.shape(0);

    clock_t st,ed;

    st = clock();
    boost::python::str *arr = reinterpret_cast<boost::python::str*>(a.get_data());
    ed = clock();
    std::cout << "get data: " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    st = clock();
    vector<int> p;
    p.resize(len);
    for (int i = 0; i < len; i++) {
        p[i] = i;
    }
    ed = clock();
    std::cout << "init p: " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    st = clock();
    sort(p.begin(), p.end(), [arr, len](int a, int b) {
            return arr[a] < arr[b];
            });
    ed = clock();
    std::cout << "sort string : " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    st = clock();
    Py_intptr_t shape[1] = {len};
    np::ndarray result = np::zeros(1,shape, np::dtype::get_builtin<int>());
    vector<int> result_vt;
    result_vt.resize(len);
    int cnt = 0;
    int last = 0;
    for (int i = 0; i < len; i++) {
        if (i == 0 || arr[p[i]] != arr[p[i - 1]]) {
            for (int j = last; j < i; j++) {
                result_vt[p[j]] = cnt;
            }
            cnt = 1;
            last = i;
        } else cnt++;
    }
    for (int j = last; j < len; j++) {
        result_vt[p[j]] = cnt;
    }
    std::copy(result_vt.begin(), result_vt.end(), reinterpret_cast<int*>(result.get_data()));
    ed = clock();
    std::cout << "compute result : " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    /*
    vector<int> pr;
    pr.resize(len);

    for (int i = 0; i < len; i++) {
        if (p[i].first == "nan" || p[i].second == "nan") {
            pr[i] = 0;
        } else {
            pr[i] = m[p[i]];
        }
    }
    */
    clock_t all_ed = clock();
    std::cout << "all in c : " << 1.0 * (all_ed - all_st) / CLOCKS_PER_SEC <<std::endl;
    return result;

}

np::ndarray compute_by_sort(np::ndarray &a) {
    clock_t all_st = clock();
    int len = a.shape(0);

    clock_t st,ed;

    st = clock();
    boost::python::str *arr = reinterpret_cast<boost::python::str*>(a.get_data());
    ed = clock();
    std::cout << "get data: " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    st = clock();
    vector<pair<string, int> > s;
    s.resize(len);
    for (int i = 0; i < len; i++) {
        s[i].first = boost::python::extract<std::string>(arr[i]);
        s[i].second = i;
    }
    ed = clock();
    std::cout << "string convert: " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    st = clock();
    sort(s.begin(), s.end());

    ed = clock();
    std::cout << "sort string : " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    st = clock();
    Py_intptr_t shape[1] = {len};
    np::ndarray result = np::zeros(1,shape, np::dtype::get_builtin<int>());
    vector<int> result_vt;
    result_vt.resize(len);
    int cnt = 0;
    int last = 0;
    for (int i = 0; i < len; i++) {
        if (i == 0 || s[i].first != s[i - 1].first) {
            for (int j = last; j < i; j++) {
                result_vt[s[j].second] = cnt;
            }
            cnt = 1;
            last = i;
        } else cnt++;
    }
    for (int j = last; j < len; j++) {
        result_vt[s[j].second] = cnt;
    }
    std::copy(result_vt.begin(), result_vt.end(), reinterpret_cast<int*>(result.get_data()));
    ed = clock();
    std::cout << "compute result : " << 1.0 * (ed - st) / CLOCKS_PER_SEC <<std::endl;

    clock_t all_ed = clock();
    std::cout << "all in c : " << 1.0 * (all_ed - all_st) / CLOCKS_PER_SEC <<std::endl;
    return result;

}

BOOST_PYTHON_MODULE(libaa) {
    Py_Initialize();
    np::initialize();
    def("convert", &convertToHex);
    def("compute_by_sort", &compute_by_sort);
    def("compute_by_sort_in_boost", &compute_by_sort_in_boost);
    def("compute_by_map", &compute_by_map);
    def("compute_by_map_no_merge", &compute_by_map_no_merge);
    def("compute_by_sort_no_merge", &compute_by_sort_no_merge);
}

/*
char str[100];
map<string, int> m;
vector<pair<string, int>> s;
vector<int> res;

int main() {
    int num = 0;
    while (scanf("%s", str) != EOF) {
        s.push_back(make_pair(string(str), num));
    }
    cout << "--" << endl;
    res.resize(s.size());
    clock_t st= clock();
    int count = 0;
    sort(s.begin(), s.end());
    for (int i = 0; i < s.size(); i++) {
        if (i ==0 || s[i].first != s[i - 1].first) {
            if (i) res[i - 1] = count;
            count = 1;
        } else {
            count++;
        }
    }
    res[s.size() - 1] = count;
    for (int i = s.size(); i >= 0; i--) {
        if (!res[i]) res[i] = res[i + 1];
    }
    clock_t ed= clock();
    cout << 1.0 * (ed-st)/CLOCKS_PER_SEC << endl;
}
*/
