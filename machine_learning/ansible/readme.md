sudo pip install --upgrade pip
sudo pip install ansible
sudo yum install git
git clone https://github.com/ICRAR/aws-chiles02.git

cd /home/ec2-user/aws-chiles02/machine_learning/ansible
ansible-playbook -i hosts site.yml
