 /*******************************************************************************
 The MIT License (MIT)

 Copyright (c) 2009-2016, 2023 Hajime Nakagami

 Permission is hereby granted, free of charge, to any person obtaining a copy of
 this software and associated documentation files (the "Software"), to deal in
 the Software without restriction, including without limitation the rights to
 use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 the Software, and to permit persons to whom the Software is furnished to do so,
 subject to the following conditions:

 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *******************************************************************************/

// 1. Get copy of Firebird 5 sources or at least src/include from Firebird 5 sources
// 2. cc -I/path/to/firebird/src/include errmsgs.c
// 3. ./a.out
// 4. perl -pi -e 's/\@\d+/~s/g' ../src/efirebirdsql_errmsgs.erl

#include <stdio.h>
#include <stdint.h>
#include "firebird/impl/msg_helper.h"

typedef unsigned short USHORT;
typedef USHORT ISC_USHORT;
typedef intptr_t ISC_STATUS;
typedef long SLONG;

FILE *fp;

#define stringify_literal(x) #x

#define FB_IMPL_MSG_NO_SYMBOL(facility, number, text)

#define FB_IMPL_MSG_SYMBOL(facility, number, symbol, text)

#define FB_IMPL_MSG(facility, number, symbol, sqlCode, sqlClass, sqlSubClass, text) \
    output_message(make_isc_code(FB_IMPL_MSG_FACILITY_##facility, number), stringify_literal(text));

int make_isc_code(int facility, int code) {
    ISC_USHORT t1 = facility;
    t1 &= 0x1F;
    ISC_STATUS t2 = t1;
    t2 <<= 16;
    ISC_STATUS t3 = code;
    code &= 0x3FFF;
    return t2 | t3 | ((ISC_STATUS) 0x14000000);
}

void process_messages() {
    #include "firebird/impl/msg/all.h"
}

void output_message(int code, char* msg) {
    fprintf(fp, "get_error_msg(%d) -> %s;\n", code, msg);
}

int main(int argc, char *argv[])
{
    int i;
    fp = fopen("../src/efirebirdsql_errmsgs.erl", "w");

    fprintf(fp, "\
\%% The contents of this file are subject to the Interbase Public\n\
\%% License Version 1.0 (the \"License\"); you may not use this file\n\
\%% except in compliance with the License. You may obtain a copy\n\
\%% of the License at http://www.Inprise.com/IPL.html\n\
\%% \n\
\%% Software distributed under the License is distributed on an\n\
\%% \"AS IS\" basis, WITHOUT WARRANTY OF ANY KIND, either express\n\
\%% or implied. See the License for the specific language governing\n\
\%% rights and limitations under the License.\n\n");
    fprintf(fp, "-module(efirebirdsql_errmsgs).\n\n-export([get_error_msg/1]).\n\n");
    process_messages();
    fprintf(fp, "get_error_msg(_) -> \"\".\n");

    fclose(fp);
    return 0;
}
