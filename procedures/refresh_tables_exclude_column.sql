USE DATABASE UTIL;
USE SCHEMA PUBLIC;
CREATE OR REPLACE PROCEDURE UTIL.PUBLIC.REFRESH_TABLES_EXCLUDE_COLUMN(
  SRC_DB VARCHAR,
  SRC_SCHEMA VARCHAR,
  TGT_DB VARCHAR,
  TGT_SCHEMA VARCHAR,
  TABLE_LIST ARRAY,
  EXCLUDE_MAP VARIANT
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
function qi(x){return '"' + x.replace(/"/g,'""') + '"';}
function exec(sql){ snowflake.createStatement({sqlText: sql}).execute(); }
function q(sql, binds){
  const st = snowflake.createStatement({sqlText: sql, binds: binds||[]});
  const rs = st.execute(); const out = [];
  while (rs.next()) out.push(rs.getColumnValue(1));
  return out;
}
function cols(db, schema, table){
  const sql = `select column_name
                 from ${qi(db)}.information_schema.columns
                where table_schema = ? and table_name = ?
                order by ordinal_position`;
  return q(sql,[schema,table]).map(c => c.toUpperCase());
}
function up(a){ return (a||[]).map(x => x.toString().toUpperCase()); }
exec('CREATE SCHEMA IF NOT EXISTS ' + qi(TGT_DB) + '.' + qi(TGT_SCHEMA));
if (!TABLE_LIST || TABLE_LIST.length === 0) return 'No tables provided';
const msgs = [];
const starEx = EXCLUDE_MAP && EXCLUDE_MAP[''] ? up(EXCLUDE_MAP['']) : [];
for (let i=0; i<TABLE_LIST.length; i++){
  let TBL = (TABLE_LIST[i]||'').toString().trim().toUpperCase();
  if (!TBL) continue;
  const src = ⁠ ${qi(SRC_DB)}.${qi(SRC_SCHEMA)}.${qi(TBL)} ⁠;
  const tgt = ⁠ ${qi(TGT_DB)}.${qi(TGT_SCHEMA)}.${qi(TBL)} ⁠;
  exec('CREATE TABLE IF NOT EXISTS ' + tgt + ' LIKE ' + src);
  const sCols = cols(SRC_DB, SRC_SCHEMA, TBL);
  const tCols = cols(TGT_DB, TGT_SCHEMA, TBL);
  const common = sCols.filter(c => tCols.indexOf(c) >= 0);
  if (common.length === 0){
    msgs.push(TBL + ': no common columns; skipped');
    continue;
  }
  const key1 = ⁠ ${SRC_SCHEMA.toUpperCase()}.${TBL} ⁠;
  const key2 = TBL;
  let ex = [];
  if (EXCLUDE_MAP){
    if (EXCLUDE_MAP[key1]) ex = ex.concat(up(EXCLUDE_MAP[key1]));
    if (EXCLUDE_MAP[key2]) ex = ex.concat(up(EXCLUDE_MAP[key2]));
    if (starEx.length) ex = ex.concat(starEx);
  }
  const exSet = new Set(ex);
  const included = common.filter(c => !exSet.has(c));
  if (included.length === 0){
    msgs.push(TBL + ': all columns excluded; skipped');
    continue;
  }
  const colList = included.map(qi).join(', ');
  exec('TRUNCATE TABLE ' + tgt);
  exec('INSERT INTO ' + tgt + ' (' + colList + ') SELECT ' + colList + ' FROM ' + src);
  msgs.push(TBL + ': refreshed (' + included.length + ' columns)');
}
return msgs.join('; ');
$$;
