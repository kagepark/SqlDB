#echo "[distutils] 
#index-servers=pypi
#[pypi] 
#repository = https://upload.pypi.org/legacy/ 
#username =javatechy" > ~/.pypirc
#python -m kmisc upload dist/*

########################################
# test.pypi.org (pip# install -i https://test.pypi.org/simple/ <package>)
########################################
# Test upload
#python3 -m twine upload --repository testpypi dist/*

# Real upload
#python3 -m twine upload --repository-url https://test.pypi.org/legacy/ --repository legacy dist/*

########################################
# pypi.org (pip# install <package>)
########################################
python3 -m twine upload --verbose --repository-url https://upload.pypi.org/legacy/ --repository legacy dist/* 
