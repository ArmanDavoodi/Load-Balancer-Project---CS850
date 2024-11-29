#ifndef TESTLOG_H
#define TESTLOG_H

#define ENABLE_TEST_LOGGING

#define COLOR_BLACK "\033[0;30m"
#define COLOR_RED "\033[0;31m"
#define COLOR_GREEN "\033[0;32m"
#define COLOR_YELLOW "\033[0;33m"
#define COLOR_BLUE "\033[0;34m"
#define COLOR_PURPLE "\033[0;35m"
#define COLOR_CYAN "\033[0;36m"
#define COLOR_WHITE "\033[0;37m"
#define COLOR_RESET "\033[0m"


#ifdef ENABLE_TEST_LOGGING
// #include <iostream>
#include <stdio.h>
// #include <string>
// #include <sstream>

#define LOGF(out_stream ,format, args...) std::fprintf(out_stream, format, ## args)
#define LOGFC(color, out_stream, format, args...) std::fprintf(out_stream, color format COLOR_RESET, ## args)
#define LOGERR(out_stream ,format, args...) LOGFC(COLOR_RED, out_stream, format, ## args)
#define LOGWAR(out_stream ,format, args...) LOGFC(COLOR_YELLOW, out_stream, format, ## args)

#else

#define LOGF(out_stream ,format, args...)
#define LOGFC(color, out_stream, format, args...)
#define LOGERR(out_stream ,format, args...)
#define LOGWAR(out_stream ,format, args...)

#endif

#endif