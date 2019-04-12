clear

for sql_file in [01]*.sql
do
    psql -f $sql_file
    echo
    read -p "Hit enter to continue"
    echo '----------------------------------------------------------------------'
done

psql -v incr=10 -f q_xmin_incrs.sql
echo '--------------------------------------------------------------------------------'
