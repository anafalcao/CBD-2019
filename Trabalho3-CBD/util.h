#ifndef UTIL_H
#define UTIL_H

#include <iostream>
#include <string>
#include <sstream>
#include <vector>

using namespace std;

void split(const string &s, char delim, vector<string> &elems) {
    stringstream ss;
    ss.str(s);
    string item;
    while (getline(ss, item, delim)) {
        elems.push_back(item);
    }
}

vector<string> split(const string &s, char delim) {
    vector<string> elems;
    split(s, delim, elems);
    return elems;
}

template <typename T>
void print(vector<T> *row) {
    for (typename vector<T>::iterator it = row->begin(); it != row->end(); it++) {
        cout << (*it) << " | ";
    }
    cout << endl;
}

 template <typename T>
 void print(vector<vector<T> > * matrix) {
     for (typename vector<vector<T> >::iterator it = matrix->begin(); it != matrix->end(); it++) {
         print(&(*it));
     }
 }

#endif //UTIL_H