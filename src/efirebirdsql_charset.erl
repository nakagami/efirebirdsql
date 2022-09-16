%%% The MIT License (MIT)
%%% Copyright (c) 2022- Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_charset).

-export([get_database_charset/1]).

-spec get_database_charset(string()) -> string().
get_database_charset("utf_8") -> "UTF8";
get_database_charset("shift_jis") -> "SJIS_0208";
get_database_charset("euc_jp") -> "EUCJ_0208";
get_database_charset("cp727") -> "DOS737";
get_database_charset("cp437") -> "DOS437";
get_database_charset("cp850") -> "DOS850";
get_database_charset("cp865") -> "DOS865";
get_database_charset("cp860") -> "DOS860";
get_database_charset("cp863") -> "DOS863";
get_database_charset("cp775") -> "DOS775";
get_database_charset("cp862") -> "DOS862";
get_database_charset("cp864") -> "DOS864";
get_database_charset("iso8859_1") -> "ISO8859_1";
get_database_charset("iso8859_2") -> "ISO8859_2";
get_database_charset("iso8859_3") -> "ISO8859_3";
get_database_charset("iso8859_4") -> "ISO8859_4";
get_database_charset("iso8859_5") -> "ISO8859_5";
get_database_charset("iso8859_6") -> "ISO8859_6";
get_database_charset("iso8859_7") -> "ISO8859_7";
get_database_charset("iso8859_8") -> "ISO8859_8";
get_database_charset("iso8859_9") -> "ISO8859_9";
get_database_charset("iso8859_13") -> "ISO8859_13";
get_database_charset("euc_kr") -> "KSC_5601";
get_database_charset("cp852") -> "DOS852";
get_database_charset("cp857") -> "DOS857";
get_database_charset("cp861") -> "DOS861";
get_database_charset("cp866") -> "DOS866";
get_database_charset("cp869") -> "DOS869";
get_database_charset("cp1250") -> "WIN1250";
get_database_charset("cp1251") -> "WIN1251";
get_database_charset("cp1252") -> "WIN1252";
get_database_charset("cp1253") -> "WIN1253";
get_database_charset("cp1254") -> "WIN1254";
get_database_charset("big5") -> "BIG_5";
get_database_charset("gb2312") -> "GB_2312";
get_database_charset("cp1255") -> "WIN1255";
get_database_charset("cp1256") -> "WIN1256";
get_database_charset("cp1257") -> "WIN1257";
get_database_charset("koi8_r") -> "KOI8R";
get_database_charset("koi8_u") -> "KOI8U";
get_database_charset("cp1258") -> "WIN1258";
get_database_charset(_) -> "UTF8".
