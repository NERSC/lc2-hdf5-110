#include <cstdio>
#include <cstdlib>
#include "hdf5.h"
#include <iostream>
#include <vector>
#include <string>

#include "params.h"
using namespace std;

int main(int argc, const char *argv[]) {
  int num_args = 2 + num_writers;
  if (argc != num_args) {
    fprintf(stderr, "usage: outputfile writer_file_1 writer_file_2 ...\n");
    return -1;
  }

  const char *master_outputfile = argv[1];
  std::vector<std::string> writer_files;
  for (int idx=0; idx < num_writers; ++idx) {
    writer_files.push_back(std::string(argv[2+idx]));
  }

  hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST);
  hid_t fid = H5Fcreate(master_outputfile, H5F_ACC_TRUNC, H5P_DEFAULT, fapl);

  int64_t fill = 0xaabbccddeeff;
  hid_t dcpl = H5Pcreate(H5P_DATASET_CREATE);
  H5Pset_fill_value(dcpl, H5T_NATIVE_INT64, &fill);
 
  const int rank1 = 1;
  hsize_t size0 = 0;
  hsize_t unlimited = H5S_UNLIMITED, countUnlimited=H5S_UNLIMITED, blockUnlimited=H5S_UNLIMITED;
  hsize_t start0=0, block1=1, count1=1, stride1=1;

  hid_t src_space = H5Screate_simple(rank1, &size0, &unlimited);
  H5Sselect_hyperslab(src_space, H5S_SELECT_SET, &start0, &stride1, &count1, &blockUnlimited);
  
  hid_t vds_space = H5Screate_simple(rank1, &size0, &unlimited);
  
  for (size_t writer = 0; writer < writer_files.size(); ++writer) {
    hsize_t vds_stride = num_writers;
    hsize_t vds_start = writer;
    
    H5Sselect_hyperslab(vds_space, H5S_SELECT_SET, &vds_start, &vds_stride, &countUnlimited, &block1);
    H5Pset_virtual(dcpl, vds_space, writer_files.at(writer).c_str(), "/data", src_space);
  }
  hid_t access_id = H5Pcreate(H5P_DATASET_ACCESS);
  H5Pset_virtual_view( access_id, H5D_VDS_FIRST_MISSING);
  hid_t dset = H5Dcreate2(fid, "vds", H5T_NATIVE_INT64, vds_space, 
                          H5P_DEFAULT, dcpl, access_id);

  // don't know if I need this since the master is not updating the file and is about to 
  // close it
  H5Fstart_swmr_write(fid);

  H5Dclose(dset);
  H5Pclose(access_id);
  H5Sclose(src_space);
  H5Sclose(vds_space);
  H5Pclose(dcpl);
  H5Fclose(fid);
  H5Pclose(fapl);

  std::cout << "master done" << std::endl;
  return 0;
}

