This directory contains a ansible playbook that will set up a cluster
for Jepsen in Google Compute Engine, run the test against it, and then
tear it down.

This instructions assume that you are running on linux. They have only
been tested with Debian 9.

Preemptible instances are used so it is possible that a node will go
away in the middle of the test.

# Setting up

Install ansible and apache-libcloud. To install to your homedir,
specify _--user_. _~/.local/bin_ must be in your PATH.

```
pip install --user ansible apache-libcloud
```

Create a service account in the GCE dashboard for your project.
See http://docs.ansible.com/ansible/latest/guide_gce.html for
instructions how. The service account should be given "Compute Admin"
and "Service Account Actor" permissions.

Copy _vars/credentials.yaml.sample_ to _vars/credentials.yaml_ and
fill in the fields with your own values.

# Running the playbook

To run the whole setup, test and teardown, do:
```
export ANSIBLE_HOST_KEY_CHECKING=False
ansible-playbook jepsen.yaml
```

If ansible exits successfully, then jepsen has passed. In all cases,
after the test, a tarball of the results will be downloaded, and
stored in the root directory of the playbook.

There are also 3 tags in the playbook, so that you can run the stages
independently. They are:
 - setup
 - test
 - teardown

With these, the test can be run multiple times, or the cluster can be
brought up for manual testing. If integrated into CI, and second
playbook run with the teardown tag should always be run, to ensure
that the nodes have been cleaned up.

```
ansible-playbook jepsen.yaml --tags teardown
```
