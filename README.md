# petrel_exp2
kafka to storm-petrel to mongodb exp

---

# Requirement:
Python 2.7.6  
python-virtualenv  
apache-storm-0.9.4  
zookeeper-3.4.7  

---

# 使用virtualenv建立虛擬python環境
建立: virtualenv 環境名稱  
啟用: source 環境名稱/bin/activate  
停用: deactivate  
在windows + vagrant環境下, 建立環境需加上--always-copy, 否則會發生protocol error  

---

# 下載petrel
easy_install petrel==0.9.4.0.3  
版本前三碼為storm版本號, 後二碼為petrel版本號  

---

# 建議
先建立python virtual env  
在virtual env內下載petrel  
後續執行皆在virtual env內進行  

---

# virtualenv 內其他3rd party 套件
pymongo  
pykafka

---

# Quick Start (on ubuntu vagrant)
sudo apt-get install python-virtualenv  
virtualenv --always-copy petrel_env  
source petrel_env/bin/activate  
easy_install petrel  
pip install pymongo pykafka  
cd ~/path/to/this/topology  
./run.sh  