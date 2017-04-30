#include <cstdio>
#include <cstdlib>
#include "hdf5.h"
#include "hdf5_hl.h"
#include <iostream>
#include "params.h"

int main(int argc, char *argv[]) {
  if (argc != 3) {
    fprintf(stderr, "usage: writer i outputfile\n");
    fprintf(stderr, "  where i = writer number\n");
    fprintf(stderr, "        outputfile = name of output file\n");
    return -1;
  }

  const int writer = atoi(argv[1]);
  const char *output_file_name = argv[2];

  hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST);
  hid_t fid = H5Fcreate(output_file_name, H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
  
  int rank1=1;
  hsize_t size0=0, unlimited=H5S_UNLIMITED;
  hid_t space_id = H5Screate_simple(rank1, &size0, &unlimited);
  hid_t plist_id = H5Pcreate(H5P_DATASET_CREATE);
  H5Pset_chunk(plist_id, rank1, &chunk_size);
  
  hid_t access_id = H5Pcreate(H5P_DATASET_ACCESS);
  size_t rdcc_nslots = 101;
  //size_t rdcc_nbytes = chunk_size * 24;
  size_t rdcc_nbytes = 0; //changed by jialin according to Quincey's suggestiosn
  double rdcc_w0 = 0.75;
  H5Pset_chunk_cache(access_id, rdcc_nslots, rdcc_nbytes, rdcc_w0);

  hid_t dset = H5Dcreate2(fid, "data", H5T_NATIVE_INT64, space_id, 
                          H5P_DEFAULT, plist_id, access_id);

  H5Fstart_swmr_write(fid);

  for (int64_t elem = writer; elem < master_len; elem += num_writers) {
    H5DOappend(dset, H5P_DEFAULT, 0, 1, H5T_NATIVE_INT64, &elem);
    usleep(microseconds_between_writes);
    if ((elem % flush_interval) == 0) {
      H5Dflush(dset);
    }
  }
  H5Dflush(dset);

  H5Dclose(dset);
  H5Pclose(access_id);
  H5Pclose(plist_id);
  H5Sclose(space_id);
  H5Fclose(fid);
  H5Pclose(fapl);

  std::cout << "writer " << writer << " done" << std::endl;
  return 0;
}

