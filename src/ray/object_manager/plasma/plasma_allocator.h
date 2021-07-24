// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include "ray/object_manager/plasma/allocator.h"

namespace plasma {

// PlasmaAllocator that allocates memory from mmapped file to
// enable memory sharing between processes.
//
// PlasmaAllocator is optimized for linux. On linux,
// the Allocate call allocates memory from a pre-mmap file
// from /dev/shm. On other system, it allocates memory from
// a pre-mmap file on disk.
//
// The FallbackAllocate always allocates memory from a disk
// based mmapped file.
class PlasmaAllocator : public IAllocator {
 public:
  PlasmaAllocator(const std::string &plasma_directory,
                  const std::string &fallback_directory, bool hugepage_enabled,
                  int64_t footprint_limit, bool fallback_enabled);

  /// On linux, it allocates memory from a pre-mmapped file from /dev/shm.
  /// On other system, it allocates memory from a pre-mmapped file on disk.
  /// return null if running out of space.
  ///
  /// \param bytes Number of bytes.
  /// \return allocated memory. returns empty if not enough space.
  absl::optional<Allocation> Allocate(size_t bytes) override;

  /// Fallback allocate memory from disk mmapped file.
  /// On linux with fallocate support, it returns null if running out of
  /// space; On linux without fallocate it raises SIGBUS interrupt.
  /// TODO(scv119): On other system the behavior of running out of space is
  /// undefined.
  ///
  /// \param bytes Number of bytes.
  /// \return allocated memory. returns empty if not enough space.
  absl::optional<Allocation> FallbackAllocate(size_t bytes) override;

  /// Frees the memory space pointed to by mem, which must have been returned by
  /// a previous call to Allocate/FallbackAllocate or it yield undefined behavior.
  ///
  /// \param allocation allocation to free.
  void Free(Allocation allocation) override;

  /// Get the memory footprint limit for this allocator.
  int64_t GetFootprintLimit() const override;

  /// Get the number of bytes allocated so far.
  int64_t Allocated() const override;

  /// Get the number of bytes fallback allocated so far.
  int64_t FallbackAllocated() const override;

 private:
  const int64_t kFootprintLimit;
  const size_t kAlignment;
  const bool kFallbackEnabled;
  int64_t allocated_;
  int64_t fallback_allocated_;
};

}  // namespace plasma
