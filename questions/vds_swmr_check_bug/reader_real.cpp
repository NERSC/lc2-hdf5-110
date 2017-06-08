#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include "hdf5.h"
#include "hdf5_hl.h"

#include "params_real.h"

hid_t H5Fopen_with_polling(const char *fname, unsigned flags, hid_t fapl_id) {
  const int one_second_in_milliseconds = 1000000;
  int waited_so_far = 0;

  /* Save old error handler */
  H5E_auto2_t old_func;
  void *old_client_data;
  H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
  
  /* Turn off error handling */
  H5Eset_auto2(H5E_DEFAULT, NULL, NULL);

  hid_t h5 = -1;
  while (true) {
    if (waited_so_far > max_seconds_reader_waits_for_master * one_second_in_milliseconds) {
      std::cout << "timeout waiting for " << fname << std::endl;
      H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);
      throw std::runtime_error("timeout");
    }
    h5 = H5Fopen(fname, flags, fapl_id);
    if (h5 >= 0) {
      break;
    }
    usleep(reader_microseconds_to_wait_when_polling_for_master);
    waited_so_far += reader_microseconds_to_wait_when_polling_for_master;
  }
  
  /* Restore previous error handler */
  H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);
	
  return h5;
}

hid_t open_dset(hid_t parent, const char *name) {
  hsize_t chunk_cache_bytes = sizeof(int64_t) * num_writers * chunk_size * 2;
  hid_t access_id = H5Pcreate(H5P_DATASET_ACCESS);

  size_t rdcc_nslots = 101;
  size_t rdcc_nbytes = chunk_cache_bytes;
  double rdcc_w0 = 0.75;
  H5Pset_chunk_cache(access_id, rdcc_nslots, rdcc_nbytes, rdcc_w0);
  H5Pset_virtual_view( access_id, H5D_VDS_FIRST_MISSING);
  hid_t dset = H5Dopen2(parent, name, access_id);
  H5Pclose(access_id);

  return dset;
}

void wait_for_dset_to_get_to_length(hid_t dset, hsize_t len) {
  hsize_t dim = 0;
  H5LDget_dset_dims(dset, &dim);
  hid_t fi;
  int microseconds_waited_so_far = 0;
  while (true) {
    //std::cout<<"updted dim:"<<dim<<"asked for:"<<len<<std::endl;
    if (dim >=len) return;
    if (microseconds_waited_so_far > max_reader_wait_microseconds) {
      return;
      throw std::runtime_error("reader wait timeout");
    }
    usleep(microseconds_between_reader_wait);
    microseconds_waited_so_far += microseconds_between_reader_wait;
    fi=H5Drefresh(dset);
      
    H5LDget_dset_dims(dset, &dim);
  }
}

int64_t read_from_dset(hid_t dset, hsize_t idx) {
  hid_t filespace = H5Dget_space(dset);
  //std::cout<<"idx:"<<idx<<std::endl;
  hsize_t start = idx;
  hsize_t stride = 1;
  hsize_t count = 1;
  hsize_t block = 1;
  hsize_t one = 1;

  H5Sselect_hyperslab(filespace,
                      H5S_SELECT_SET,
                      &start,
                      &stride,
                      &count,
                      &block);
  
  hid_t memspace = H5Screate_simple(one, &one, &one);
  H5Sselect_all(memspace);

  int64_t data;

  size_t nelmts = 0;
  hid_t dcpl;
  dcpl = H5Dget_create_plist (dset);
  unsigned int    flags, filter_info;
  int nfilter = H5Pget_nfilters(dcpl);
  //add filter later
  herr_t status=H5Dread(dset, H5T_NATIVE_INT64, memspace, filespace, H5P_DEFAULT, &data);
  if(status<0) printf("H5Dread returns: %i\n",status);
  H5Sclose( filespace );
  H5Sclose( memspace );
  //std::cout<<" start: "<<start<<" stride:"<<stride<<" count:"<<count<<" block:"<<block<<" value:"<<data<<std::endl;
  return data;
}
//  std::cout<<"number of filters:"<<nfilter<<std::endl;
//  H5Z_filter_t  filter_type = H5Pget_filter (dcpl, 0, &flags, &nelmts, NULL, 0, NULL,
//                &filter_info);
//  std::cout<<"Filter type is: "<<std::endl;
/*  switch (filter_type) {
        case H5Z_FILTER_DEFLATE:
            std::cout<<"H5Z_FILTER_DEFLATE"<<std::endl;
            break;
        case H5Z_FILTER_SHUFFLE:
            printf ("H5Z_FILTER_SHUFFLE\n");
            break;
        case H5Z_FILTER_FLETCHER32:
            printf ("H5Z_FILTER_FLETCHER32\n");
            break;
        case H5Z_FILTER_SZIP:
            printf ("H5Z_FILTER_SZIP\n");
            break;
        case H5Z_FILTER_NBIT:
            printf ("H5Z_FILTER_NBIT\n");
            break;
        case H5Z_FILTER_SCALEOFFSET:
            printf ("H5Z_FILTER_SCALEOFFSET\n");
    }
*/
int main(int argc, char *argv[]) {
  if (argc != 3) {
    fprintf(stderr, "usage: reader i inputfile\n");
    fprintf(stderr, "  where i = reader number\n");
    fprintf(stderr, "        inputfile = name master file\n");
    return -1;
  }

  const int reader = atoi(argv[1]);
  const char *input_file_name = argv[2];

  hid_t fid = H5Fopen_with_polling(input_file_name, 
                                   H5F_ACC_RDONLY | H5F_ACC_SWMR_READ, 
                                   H5P_DEFAULT);
  
  hid_t dset = open_dset(fid, "data");

//  for (int64_t idx = 0; idx < master_len; ++idx) {
  for (int64_t idx = 0; idx < writer_len; ++idx) {
    //if ((idx % num_readers) != reader) {
    //  continue;
    //}
    wait_for_dset_to_get_to_length(dset, idx+1);
   // std::cout<<"reader "<<reader<<" before read, idx:"<<idx<<std::endl;
    int64_t value = read_from_dset(dset, idx);
   // std::cout<<"reader "<<reader<<" after read idx:"<<idx<<std::endl;
    if (value != idx) {
      std::cout << "ERROR reader:" << reader << " read 0x" << std::hex << value 
                << " != 0x" << std::hex << idx << " for entry " 
                << std::dec << idx << " of vds" << std::endl;
      std::cout.flush();
    }   
  }
  H5Dclose(dset);
  H5Fclose(fid);
  std::cout << "reader " << reader << " done" << std::endl;
  return 0;
}

