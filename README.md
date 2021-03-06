<p align="center">
<img alt="Kale Logo" src="https://raw.githubusercontent.com/kubeflow-kale/kale/master/docs/imgs/kale_logo.png" height="130">
</p>
<p align="center">
<img alt="GitHub License" src="https://img.shields.io/github/license/kubeflow-kale/kale"> <img alt="PyPI Version" src="https://img.shields.io/pypi/v/kubeflow-kale"> <img alt="npm Version" src="https://img.shields.io/npm/v/kubeflow-kale-launcher"> <img alt="GitHub Workflow Status" src="https://img.shields.io/github/workflow/status/kubeflow-kale/kale/CI"> <img alt="GitHub commits since tagged version" src="https://img.shields.io/github/commits-since/kubeflow-kale/kale/v.0.4.0"> 
</p>

---

KALE (Kubeflow Automated pipeLines Engine) is a project that aims at simplifying the Data Science experience of deploying Kubeflow Pipelines workflows.

Kubeflow is a great platform for orchestrating complex workflows on top Kubernets and Kubeflow Pipeline provides the mean to create reusable components that can be executed as part of workflows. The self-service nature of Kubeflow make it extremely appealing for Data Science use, at it provides an easy access to advanced distrubted jobs orchestration, reausability of components, Jupuyter Notebooks, rich UIs and more. Still, developing and maintaining Kubeflow workflows can be hard for data sciencets, who may not be experts in working orchestration platforms and related SDKs. Additionally, data science often involve processes of data exploration, iterative modelling and interactve environents (mostly Jupyter notebook).

Kale bridges this gap by providing a simple UI to define Kubeflow Pipelines workflows directly from you JupyterLab interface, without the need to change a single line of code.

Read more about Kale and how it works in this Medium post: [Automating Jupyter Notebook Deployments to Kubeflow Pipelines with Kale](https://medium.com/kubeflow/automating-jupyter-notebook-deployments-to-kubeflow-pipelines-with-kale-a4ede38bea1f)

## Getting started

Install the Kale backend from PyPI and the JupyterLab extension. You can find a set of curated Notebooks in the [examples repository](https://github.com/kubeflow-kale/examples)

```bash
# install kale
pip install kubeflow-kale

# install jupyter lab
pip install "jupyterlab<2.0.0"

# install the extension
jupyter labextension install kubeflow-kale-launcher
# verify extension status
jupyter labextension list

# run
jupyter lab
```

<img alt="Kale Labextension" src="https://raw.githubusercontent.com/kubeflow-kale/kale/master/docs/imgs/labextension.png"/>

To build images to be used as a NotebookServer in Kubeflow, refer to the Dockerfile in the `docker` folder.

## Resources

- Kale introduction [blog post](https://medium.com/kubeflow/automating-jupyter-notebook-deployments-to-kubeflow-pipelines-with-kale-a4ede38bea1f)
- [Codelab](https://codelabs.developers.google.com/codelabs/cloud-kubeflow-minikf-kale/#0) showcasing Kale working in MiniKF with Arrikto's [Rok](https://www.arrikto.com/)
- KubeCon NA Tutorial 2019: [From Notebook to Kubeflow Pipelines: An End-to-End Data Science Workflow](https://kccncna19.sched.com/event/Uaeq/tutorial-from-notebook-to-kubeflow-pipelines-an-end-to-end-data-science-workflow-michelle-casbon-google-stefano-fioravanzo-fondazione-bruno-kessler-ilias-katsakioris-arrikto?iframe=no&w=100%&sidebar=yes&bg=no) - [video](http://youtube.com/watch?v=C9rJzTzVzvQ)
- CNCF Webinar 2020: [From Notebook to Kubeflow Pipelines with MiniKF & Kale](https://www.cncf.io/webinars/from-notebook-to-kubeflow-pipelines-with-minikf-kale/) - [video](https://www.youtube.com/watch?v=1fX9ZFWkvvs)

## Contribute

This repository uses
[husky](https://github.com/typicode/husky)
to set up git hooks.

For `husky` to function properly, you need to have `yarn` installed and in your `PATH`. The reason that is required is that `husky` is installed via `jlpm install` and `jlpm` is a `yarn` wrapper. (Similarly, if it was installed using the `npm` package manager, then `npm` would have to be in `PATH`.)

Currently installed git hooks:

- `pre-commit`: Run a prettier check on staged files, using [pretty-quick](https://github.com/azz/pretty-quick)
