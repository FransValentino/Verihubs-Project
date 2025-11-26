to install the required packages to run this project, on the terminal go to setup.py, and type the following in the terminal

```bash
    pip install .
```

once the installation is completed, go to the directory which contains the dagster project. in this case that would be the verihubs-dagster folder

to access the Dagster web UI, type the following in the terminal

```bash
    dagster dev
```

after the connection has been established, head to your web browser and go to localhost:3000 in order to access the Dagster web UI

in the Lineage menu, there should be assets related to this project. go ahead and click the leftmost asset and click materialize. once it's completed you can proceed with the other assets.