# check that we are using Python 3.x.x
py_version=$(python --version | cut -f2 -d' ')
if [[ ${py_version:0:1} != 3  ]] ; then
    echo "Set python version with 'pyenv local|global anaconda3-2019.10'"
    exit 0
fi

jupyter notebook
