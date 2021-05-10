# Generate primary and mirror arrays for GP 5 (and probably 4)

psql <<EOF > primary_array.out
select address || '~' || (port-2000)::text || '~' ||
f.fselocation || '~' || dbid || '~' || content || 
case when replication_port is not null then '~' || replication_port::text else '' end 
from gp_segment_configuration c
inner join pg_filespace_entry f
   on c.dbid = f.fsedbid
where role = 'p' and content >= 0 order by dbid
EOF

psql <<EOF > mirror_array.out
select address || '~' || (port-2000)::text || '~' ||  
f.fselocation || '~' || dbid || '~' || content || '~' || replication_port::text
from gp_segment_configuration c
inner join pg_filespace_entry f
   on c.dbid = f.fsedbid  
where role = 'm' and content >= 0 order by dbid
EOF

