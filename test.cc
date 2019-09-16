#include <iostream>
#include <boost/python.hpp>
#include <boost/python/numpy.hpp>
#include <map>
#include <ctime>
#include <vector>
#include <cstring>
#include <cstdio>
#include <algorithm>
using namespace std;
using namespace boost::python;
namespace np = boost::python::numpy;

void func(np::ndarray &a) {
    int len = a.shape(0);
    boost::python::str *arr = reinterpret_cast<boost::python::str*>(a.get_data());
    string test= boost::python::extract<std::string>(arr[1]);
    std::cout << test << std::endl;
}

BOOST_PYTHON_MODULE(libaa) {
    Py_Initialize();
    np::initialize();
    def("func", &func);
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
