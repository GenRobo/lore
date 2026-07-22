// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Virtual file system support shared across platform backends.
//!
//! [`core`] holds the platform-neutral path-resolution, directory-enumeration, and
//! content-read operations. Platform backends translate their OS filesystem API into those
//! operations: the Windows `ProjFS` provider lives in [`crate::projfs`]; the Linux FUSE backend
//! is added under this module.

pub mod core;
