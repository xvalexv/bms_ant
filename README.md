# bms_ant
BMS monitoring for ANT BMS LiFePo4 batteries

* Install:

apt install python3 python3-pip
apt install build-essential libbluetooth-dev
pip install -r requirements.txt 

* Note if you use Raspberry Pi with USB to rs485 adapter:

Requirements - x64 os system , core > 4.4
tested on:
PI3 B+
PI4
Linux PI4 5.15.32-v8+

1). Install linux headers 
apt-get install raspberrypi-kernel-headers

2). 
Download latest Ch340 driver
option 1:
git clone https://github.com/juliagoda/CH341SER
cd CH341SER

option 2:
* https://www.wch.cn/download/CH341SER_LINUX_ZIP.html
wget https://cdn.sparkfun.com/assets/learn_tutorials/8/4/4/CH341SER_LINUX.ZIP
unzip CH341SER_LINUX.ZIP
cd CH341SER_LINUX

3). Compile driver

make
make load

** Note if your battery isn't loaded, it can be in sleep mode. To get it work you must run script when battery is under load or can press Reset key on battery.