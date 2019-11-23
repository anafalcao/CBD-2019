#ifndef Schema_H
#define Schema_H

#include <iostream>
#include <fstream>
#include <map>
#include <algorithm>
#include "util.h"

using namespace std;

enum SchemaType {
    INT32,
    INT64,
    CHAR,
    FLOAT,
    DOUBLE,
    FOREIGN_KEY
    
};

struct SchemaCol {
    string key;
    SchemaType type;
    unsigned array_size;
    
    unsigned getSize() {
        switch (type) {
            case INT32:
            case FLOAT:
                return sizeof(float) * (array_size + 1);
            case INT64:
            case FOREIGN_KEY:
            case DOUBLE:
                return sizeof(double) * (array_size + 1);
            case CHAR:
                return sizeof(char) * (array_size + 1);
            default:
                return 0;
        }
    }
};


class Schema {
private:
    vector<SchemaCol> cols;
    unsigned size;
    
public:
    /**
     *@constructor
     */
    Schema();
    

    void import(const string & path);
    
     vector<SchemaCol> * getCols();
     SchemaCol * getCol(string key);
     
     int getColPosition(string key);
    
     void addCol(string key, SchemaType type);
     void addCol(string key, SchemaType type, unsigned array_size);
      

     int getNumberOfCols();
    
      unsigned getSize();
};

Schema::Schema() {
    size = -1;
    SchemaCol _id;
    _id.key = "_id";
    _id.type = INT64;
    _id.array_size = 0;
    
    cols.push_back(_id);
}

void Schema::import(const string & path) {
    ifstream file;
    file.open(path.c_str());
    string line;
    
    if (file.is_open()) {
        
        while (getline(file, line)) {
            vector<string> words = split(line, ':');
            SchemaCol col;
            unsigned size = words.size();
            
            if (size > 0) {
                
                col.key = words.at(0);
                
        
                string type = words.at(1);
                
                if (type == "int32") {
                    col.type = INT32;
                } else if (type == "char") {
                    col.type = CHAR;
                } else if (type == "float") {
                    col.type = FLOAT;
                } else if (type == "double") {
                    col.type = DOUBLE;
                } else if (type == "foreign_key") {
                    col.type = FOREIGN_KEY;
                }
                
                if (size == 3) {
                    col.array_size = atoi(words.at(2).c_str());
                } else {
                    col.array_size = 0;
                }
                
                
                cout << col.key << " " << col.type << " " << col.array_size << endl;
                cols.push_back(col);
            }
        }
        file.close();
    } else {
        cout << "Unable to open file - " << path << endl;
    }
    
}

vector<SchemaCol> * Schema::getCols() {
    return &cols;
}

SchemaCol * Schema::getCol(string key){
    return &cols.at(getColPosition(key));
}

int Schema::getColPosition(string key){
    for(int i=0; i<cols.size(); i++){
        if(cols.at(i).key == key){
            return i;
        }
    }

    return -1;
}

void Schema::addCol(string key, SchemaType type) {
    addCol(key, type, 0);
}

void Schema::addCol(string key, SchemaType type, unsigned array_size) {
    SchemaCol col;
    col.key = key;
    col.type = type;
    col.array_size = array_size;
    cols.push_back(col);
}

unsigned Schema::getSize() {
    if (size == -1) {
      
        size = 0;
        for (vector<SchemaCol>::iterator it = cols.begin(); it != cols.end(); it++) {
            size += (*it).getSize();
        }
    }
    
    return size;
}

int Schema::getNumberOfCols() {
    return cols.size();
}
 
 #endif //Schema_H
