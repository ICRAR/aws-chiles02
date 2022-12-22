# Install CASA 6 on Setonix

## Python

CASA 6.0 only works with python 3.6 or 3.8

Use miniconda to install python 3.8

```bash
cd /software/projects/pawsey0411/kvinsen/
wget https://repo.anaconda.com/miniconda/Miniconda3-py38_4.12.0-Linux-x86_64.sh
bash Miniconda3-py38_4.12.0-Linux-x86_64.sh

miniconda3/bin/python3.8 -m venv venvs/chiles
source venvs/chiles/bin/activate

pip install -U pip numpy scipy matplotlib 

pip install --index-url https://casa-pip.nrao.edu/repository/pypi-casa-release/simple casatools
pip install --index-url https://casa-pip.nrao.edu/repository/pypi-casa-release/simple casatasks
```


# Install CASA 5 on Setonix

```bash
cd /software/projects/pawsey0411/kvinsen/
wget https://casa.nrao.edu/download/distro/casa-pipeline/release/el7/casa-pipeline-release-5.6.3-19.el7.tar.gz.md5

tar -xvf casa-pipeline-release-5.6.3-19.el7.tar.gz
```