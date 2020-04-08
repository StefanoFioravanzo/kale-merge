import kubernetes
from kubernetes.client.rest import ApiException

from kale.utils import utils, pod_utils, kfp_utils
from kale.rpc.errors import RPCNotFoundError, RPCUnhandledError

KATIB_PARAMETER_NAMES = ("objective", "algorithm", "parallelTrialCount",
                         "maxTrialCount", "maxFailedTrialCount", "parameters")
KATIB_DEFAULTS = {"parallelTrialCount": 3, "maxTrialCount": 12,
                  "maxFailedTrialCount": 3}
KATIB_EXPERIMENT_STATUS = ["Failed", "Succeeded", "Restarting", "Running",
                           "Created"]

RAW_TEMPLATE = """\
apiVersion: batch/v1
kind: Job
metadata:
  name: {{.Trial}}
  namespace: {{.NameSpace}}
spec:
  backoffLimit: 0
  template:
    metadata:
      annotations:
        sidecar.istio.io/inject: "false"
    spec:
      restartPolicy: Never
      containers:
        - name: {{.Trial}}
          image: {image}
          command:
            - python3 -u -c "from kale.utils.kfp_utils\
                import create_and_wait_kfp_run;\
                create_and_wait_kfp_run(\
                    pipeline_id='{pipeline_id}',\
                    run_name='{run_name}',\
                    experiment_name='{experiment_name}',\
                    {{- with .HyperParameters }} {{- range .}}
                        {{.Name}}='{{.Value}}',\
                    {{- end }} {{- end }}\
                )"
"""


def _get_k8s_co_client():
    try:
        kubernetes.config.load_incluster_config()
    except Exception:  # Not in a notebook server
        try:
            kubernetes.config.load_kube_config()
        except Exception:
            raise RPCUnhandledError(details="Could not load Kubernetes config",
                                    trans_id=request.trans_id)

    return kubernetes.client.CustomObjectsApi()


def _define_katib_experiment(name, katib_spec, trial_parameters):
    """Define Katib experiment."""
    katib_experiment = {"apiVersion": "kubeflow.org/v1alpha3",
                        "kind": "Experiment",
                        "metadata": {"labels": {
                            "controller-tools.k8s.io": "1.0"},
                            "name": name},
                        "spec": katib_spec}

    raw_template = RAW_TEMPLATE.replace("{{", "<<<").replace("}}", ">>>")
    raw_template = raw_template.format(**trial_parameters)
    raw_template = raw_template.replace("<<<", "{{").replace(">>>", "}}")
    katib_experiment["spec"]["trialTemplate"] = {"goTemplate": {
        "rawTemplate": raw_template}}

    return katib_experiment


def _launch_katib_experiment(request, katib_experiment, namespace):
    """Launch Katib experiment."""
    k8s_co_client = _get_k8s_co_client()

    co_group = "kubeflow.org"
    co_version = "v1alpha3"
    co_plural = "experiments"

    request.log.debug("Launching Katib Experiment '%s'...",
                      katib_experiment["metadata"]["name"])
    try:
        k8s_co_client.create_namespaced_custom_object(co_group, co_version,
                                                      namespace, co_plural,
                                                      katib_experiment)
    except ApiException as e:
        request.log.exception("Failed to launch Katib experiment")
        raise RPCUnhandledError(message="Failed to launch Katib experiment",
                                details=str(e), trans_id=request.trans_id)
    request.log.info("Successfully launched Katib Experiment")


def _sanitize_parameters(request, parameters, parameter_names, defaults,
                         parameters_type):
    """Keep just the known parameter fields that are required."""
    sanitized = {}
    for param in parameter_names:
        if param not in parameters and param not in defaults:
            request.log.exception("%s parameter '%s' was not provided",
                                  parameters_type, param)
            raise RPCNotFoundError(details=("%s parameter '%s' is required"
                                            % (parameters_type, param)),
                                   trans_id=request.trans_id)
        sanitized[param] = parameters.pop(param, defaults.get(param))
    if parameters:
        request.log.debug("Ignoring %s parameters: %s", parameters_type,
                          ", ".join(parameters.keys()))
    return sanitized


def _sanitize_katib_spec(request, katib_spec):
    """Sanitize a given Katib specification."""
    return _sanitize_parameters(request, katib_spec, KATIB_PARAMETER_NAMES,
                                KATIB_DEFAULTS, "Katib")


def create_katib_experiment(request, pipeline_id, pipeline_metadata):
    """Define and launch a Katib experiment."""
    try:
        namespace = pod_utils.get_namespace()
    except Exception:
        # XXX: When not running from within a pod, get_namespace() fails
        # XXX: If that's the case, use the 'kubeflow-user' one
        # XXX: This should probably change. It works for local/MiniKF dev
        namespace = "kubeflow-user"

    pipeline_name = kfp_utils.get_pipeline(pipeline_id).name
    katib_name = "%s-katib-%s" % (utils.sanitize_k8s_name(pipeline_name),
                                  utils.random_string())
    katib_name = pipeline_metadata.get("experiment_name")

    katib_spec = pipeline_metadata.get("katib_metadata", None)
    if not katib_spec:
        raise RPCNotFoundError(details=("Could not find Katib specification in"
                                        " notebook's metadata"),
                               trans_id=request.trans_id)
    # Perform a sanitization of the Katib specification, making sure all the
    # required first-layer-fields are set
    katib_spec = _sanitize_katib_spec(request, katib_spec)

    trial_parameters = {
        "image": "gcr.io/arrikto-playground/stefano/kale/katib-trial:v.0.4.0-36-ge73725a",
        "pipeline_id": pipeline_id,
        "run_name": katib_name,
        "experiment_name": pipeline_metadata.get(
            "experiment_name")}

    katib_experiment = _define_katib_experiment(katib_name, katib_spec,
                                                trial_parameters)
    with open("./%s.yaml" % katib_name, "w") as yaml_file:
        import yaml
        yaml_text = yaml.dump(katib_experiment)
        yaml_file.write(yaml_text)
    _launch_katib_experiment(request, katib_experiment, namespace)

    return {"name": katib_experiment["metadata"]["name"],
            "namespace": namespace,
            "status": None,
            "trials": 0,
            "maxTrialCount": katib_experiment["spec"]["maxTrialCount"]}


def get_experiment(request, experiment, namespace):
    """Get Katib experiment."""
    k8s_co_client = _get_k8s_co_client()

    co_group = "kubeflow.org"
    co_version = "v1alpha3"
    co_plural = "experiments"

    try:
        exp = k8s_co_client.get_namespaced_custom_object(co_group, co_version,
                                                         namespace, co_plural,
                                                         experiment)
    except ApiException as e:
        request.log.exception("Failed to get Katib experiment")
        raise RPCUnhandledError(message="Failed to get Katib experiment",
                                details=str(e), trans_id=request.trans_id)

    return {"name": experiment,
            "namespace": namespace,
            "status": _get_experiment_status(exp["status"]),
            "trials": exp["status"].get("trials", 0),
            "trialsFailed": exp["status"].get("trialsFailed", 0),
            "trialsRunning": exp["status"].get("trialsRunning", 0),
            "trialsSucceeded": exp["status"].get("trialsSucceeded", 0),
            "maxTrialCount": exp["spec"]["maxTrialCount"]}


def _get_experiment_status(experiment_status):
    """Retrieve an experiment's status."""

    def _is_status(condition, status):
        return condition["type"] == status and condition["status"] == "True"

    for status in KATIB_EXPERIMENT_STATUS:
        for condition in experiment_status["conditions"]:
            if _is_status(condition, status):
                return status
    return None
