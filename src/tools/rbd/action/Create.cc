// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include <iostream>
#include <boost/program_options.hpp>
#include "librbd/ImageCtx.h"

namespace rbd {
namespace action {
namespace create {

namespace at = argument_types;
namespace po = boost::program_options;

static int do_create(librbd::RBD &rbd, librados::IoCtx& io_ctx,
                     const char *imgname, uint64_t size,
		     librbd::ImageOptions& opts) {
  return rbd.create4(io_ctx, imgname, size, opts);
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_create_image_options(options, true);
  options->add_options()
    (at::IMAGE_THICK.c_str(), po::bool_switch(), "thick image provisioned");
  at::add_size_option(options);
  at::add_no_progress_option(options);
}

int write_data(librbd::Image &image, bool no_progress)
{
  uint64_t concurr =
    g_conf->get_val<int64_t>("rbd_concurrent_management_ops");

  uint64_t image_size;
  const int chunk_size = 1024 * 1024; // 1MB
  const int block_size = 512; // 512 Bytes
  char write_buff[block_size];
  uint64_t chunk_num;
  uint64_t complete_count = 0;
  uint64_t target_chunk = 0;
  uint64_t write_offset;
  int max_aio = 0;
  int r = 0;
  ceph::bufferlist bl;
  librbd::RBD::AioCompletion *cvec[concurr];
  utils::ProgressContext pc("Writing data for thick provisioning", no_progress);

  if (image.size(&image_size) != 0) {
    r = -EINVAL;
    goto err_size;
  }

  chunk_num = image_size / chunk_size;

  // If write_buf has zero data, aio_writesame doesn't write
  // actual data to objects. So, write_buff is initialized by 1
  // to allocate objects by rbd command.
  memset(write_buff, 1, block_size);
  bl.append(write_buff, block_size);

  memset(cvec, 0, sizeof(cvec));

  for (;;) {
    // Calculate the max num of AIOs to send
    if ((chunk_num - target_chunk) >= concurr) {
      max_aio = concurr;
    } else {
      max_aio = (chunk_num - target_chunk);
    }

    // Send AIO
    for (int i = 0; i < max_aio; i++) {
      if (cvec[i] == NULL) {
	write_offset = target_chunk * chunk_size;

	cvec[i] = new librbd::RBD::AioCompletion(NULL, NULL);
	r = image.aio_writesame(write_offset, chunk_size, bl, cvec[i], LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL);
	if (r < 0) {
	  std::cerr << "rbd: aio_writesame returns fail value (" << r << ")" << std::endl;
	  goto err_writesame;
	}
	target_chunk++;
      }
    }

    // Check completion
    for (unsigned int j = 0; j < concurr; j++) {
      if (cvec[j] != NULL && cvec[j]->is_complete()) {
        r = cvec[j]->get_return_value();
        cvec[j]->release();
        cvec[j] = NULL;
	if (r < 0) {
	  std::cerr << "rbd: aio_writesame completion returns fail value (" << r << ")" << std::endl;
	  goto err_writesame;
	}

	complete_count++;
	pc.update_progress((complete_count*chunk_size) ,image_size);
      }
      usleep(10); // To avoid busy loop
    }

    if (complete_count == chunk_num) {
      pc.finish();
      break;
    }
  }

  return r;

err_writesame:
  // Collect IOs already sent
  for (unsigned int j = 0; j < concurr; j++) {
    if (cvec[j] != NULL && cvec[j]->is_complete()) {
      cvec[j]->release();
      cvec[j] = NULL;
    }
  }
  pc.fail();

err_size:

  return r;
}

int thick_write(const std::string &pool_name,
		      const std::string &image_name, uint64_t size,
		      librbd::ImageOptions &opts, bool no_progress)
{
  int r = 0;

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;

  r = utils::init_and_open_image(pool_name, image_name, "", "", false, &rados,
			  &io_ctx, &image);
  if (r < 0) {
    std::cerr << "rbd: Cannot initialize or open image for thick." << std::endl;
    return r;
  }

  r = write_data(image, no_progress);

  image.close();
  io_ctx.close();
  rados.shutdown();

  return r;
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_FULL);
  if (r < 0) {
    return r;
  }

  librbd::ImageOptions opts;
  r = utils::get_image_options(vm, true, &opts);
  if (r < 0) {
    return r;
  }

  uint64_t size;
  r = utils::get_image_size(vm, &size);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = do_create(rbd, io_ctx, image_name.c_str(), size, opts);
  if (r < 0) {
    std::cerr << "rbd: create error: " << cpp_strerror(r) << std::endl;
    return r;
  }

  if (vm.count(at::IMAGE_THICK) && vm[at::IMAGE_THICK].as<bool>()) {
    r = thick_write(pool_name, image_name, size, opts, vm[at::NO_PROGRESS].as<bool>());
    if (r < 0) {
      std::cerr << "rbd: image was created, but write error occurred for thick: "
		<< cpp_strerror(r) << std::endl;
      return r;
    }
  }
  return 0;
}

Shell::Action action(
  {"create"}, {}, "Create an empty image.", at::get_long_features_help(),
  &get_arguments, &execute);

} // namespace create
} // namespace action
} // namespace rbd
