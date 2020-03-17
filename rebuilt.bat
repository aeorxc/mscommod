pip uninstall -y mscommod
python setup.py bdist_wheel
pip install dist\mscommod-1.1.0-py2.py3-none-any.whl
REM pip install --upgrade git+https://github.com/aeorxc/mscommod#egg=mscommod