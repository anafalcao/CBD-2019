#ifndef QUERYABLE_H
#define QUERYABLE_H

#include "schema.h"


struct RegistryHeader {
    char table_name[255];
    unsigned registry_size;
    time_t time_stamp;
};

struct HeaderFile {
    long long _id;
    long long registry_position;
    string path;
};

typedef vector<pair<decltype(HeaderFile::_id), decltype(HeaderFile::registry_position)> > header_t;

class Queryable {
public:
  virtual vector<string> getRow(long long registry_position) =0;
  virtual vector<string> getRowById(long long _id) =0;
  virtual Schema getSchema() =0;
  virtual header_t* getHeader() =0;
  virtual vector<pair<string, long long>> *getColumn(string column_name) =0;
  virtual vector<pair<string, long long>> *getColumn(int column_position) =0;
  virtual string getValue(long long _id, int column_position) =0;
  virtual int getNumberOfRows() =0;
};

#endif 