#ifndef PARAMS_REAL_H
#define PARAMS_REAL_H

#include <unistd.h>

const int64_t writer_len = 400;
//const int num_writers = 3;
const int num_writers = 1;
const int64_t num_readers = 2;
const int64_t master_len = num_writers * writer_len;
const hsize_t chunk_size = 1;
const int microseconds_between_writes = 6;
const int microseconds_between_reader_wait = 3;
const int64_t flush_interval = 10; 

int max_seconds_reader_waits_for_master = 10;
int reader_microseconds_to_wait_when_polling_for_master = 1000;
int max_reader_wait_microseconds = 20 * 1000000;

#endif
