CREATE DATABASE IF NOT EXISTS idf;

drop table if exists idf.RAW_TABLE;
create table if not exists idf.RAW_TABLE
(
    raw_data 		String,
    _inserted_at 	DateTime DEFAULT now()
) ENGINE = MergeTree()
order by _inserted_at;

drop table if exists idf.PARSED_TABLE ;
create table if not exists idf.PARSED_TABLE (
    craft 			String,
    name 			String,
    _inserted_at 	DateTime
) ENGINE = ReplacingMergeTree(_inserted_at)
order by (craft, name);

drop view if exists idf.MV;
create materialized view idf.MV to idf.PARSED_TABLE as
select 
	 person.craft as craft
	, person.name as name
	, _inserted_at
from idf.RAW_TABLE rt
ARRAY join JSONExtract(replaceAll(rt.raw_data, '''', '"'), 'people', 'Array(Tuple(craft String, name String))') as person
;

select raw_data, rt.`_inserted_at`  from idf.RAW_TABLE rt ;
select pt.craft , pt.name , pt.`_inserted_at`  from idf.PARSED_TABLE pt ;

OPTIMIZE TABLE idf.PARSED_TABLE FINAL;

select pt.craft , pt.name , pt.`_inserted_at`  from idf.PARSED_TABLE pt ;