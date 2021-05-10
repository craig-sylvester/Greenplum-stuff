/*
 * Greenplum 4.3, 5.x
 * ******************
 * List instances where the primary and mirror segments are on the same host.
 *
 */

select s.address, s.port, s.replication_port, f.fselocation
from gp_segment_configuration s join pg_filespace_entry f on s.dbid = f.fsedbid
   , (select count(*), content, address from gp_segment_configuration
      group by content,address
      having count(*) > 1) a
where s.content = a.content
order by s.content, f.fselocation desc;
