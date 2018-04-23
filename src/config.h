/* MDBX core options
 *
 * TODO: generage by configure/CMake
 */

#pragma once
/* *INDENT-OFF* */
/* clang-format off */

/*----------------------------------------------------------------------------*/
/* Predefined configure bits */

#define MDBX_CONFIG_DBG_ASSERTIONS 1
#define MDBX_CONFIG_DBG_AUDIT 2
#define MDBX_CONFIG_DBG_JITTER 4
#define MDBX_CONFIG_DBG_DUMP 8
#define MDBX_CONFIG_DBG_LOGGING 16
#define MDBX_CONFIG_DBG_VALGRIND 32
#define MDBX_CONFIG_DBG_64 64 /* reserved */
#define MDBX_CONFIG_DBG_128 128 /* reserved */

#ifndef MDBX_CONFIGURED_DEBUG_ABILITIES
#define MDBX_CONFIGURED_DEBUG_ABILITIES 255
#endif

#ifndef MDBX_DEBUG
#   define MDBX_DEBUG 0
#endif

#if MDBX_DEBUG
#   undef NDEBUG
#endif

#ifndef MDBX_DEVEL
#   define MDBX_DEVEL 1
#endif

#ifndef MDBX_OPT_TEND_LCK_RESET
#   define MDBX_OPT_TEND_LCK_RESET 1
#endif

#ifndef MDBX_INTERNAL
#define MDBX_INTERNAL static
#endif

/* *INDENT-ON* */
/* clang-format on */
