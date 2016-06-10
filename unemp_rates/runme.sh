clear

for sqlfile in 0[1-9]*.sql
do
    psql -f $sqlfile
    echo
    read -p "Hit enter to continue"
    echo '------------------------------------------------------------------------'
done
