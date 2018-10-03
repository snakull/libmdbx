/* MDBX core options
 *
 * TODO: generage by configure/CMake
 */

#pragma once
/* *INDENT-OFF* */
/* clang-format off */

/*----------------------------------------------------------------------------*/
/* Predefined configure bits */

#define MDBX_CONFIG_ASSERTIONS    1
#define MDBX_CONFIG_LOGGING       2
#define MDBX_CONFIG_DBDUMP        4
#define MDBX_CONFIG_DBG_JITTER    8
#define MDBX_CONFIG_DBG_AUDIT    16
#define MDBX_CONFIG_DEBUG        32
#define MDBX_CONFIG_DEVEL        64
#define MDBX_CONFIG_VALGRIND    128

#ifndef MDBX_CONFIGURED_DEBUG_ABILITIES
#define MDBX_CONFIGURED_DEBUG_ABILITIES 0
#endif

#define MDBX_DEBUG (MDBX_CONFIGURED_DEBUG_ABILITIES & MDBX_CONFIG_DEBUG)
#define MDBX_DEVEL (MDBX_CONFIGURED_DEBUG_ABILITIES & MDBX_CONFIG_DEVEL)
#define MDBX_LOGGING (MDBX_CONFIGURED_DEBUG_ABILITIES & MDBX_CONFIG_LOGGING)
#define MDBX_ASSERTIONS (MDBX_CONFIGURED_DEBUG_ABILITIES & MDBX_CONFIG_ASSERTIONS)
#define MDBX_AUDIT (MDBX_CONFIGURED_DEBUG_ABILITIES & MDBX_CONFIG_DBG_AUDIT)

#ifndef MDBX_OPT_TEND_LCK_RESET
#   define MDBX_OPT_TEND_LCK_RESET 1
#endif

#ifndef MDBX_INTERNAL
#define MDBX_INTERNAL static
#endif

/* *INDENT-ON* */
/* clang-format on */
