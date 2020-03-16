# Copyright 2020 The Kale Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""ML Metadata util functions.

Implementation inspired by KFP Metadata Writer.
"""

import os
import re
import sys
import json
from time import sleep
import hashlib

from ml_metadata.proto import metadata_store_pb2
from ml_metadata.metadata_store import metadata_store

from kale.utils import utils, pod_utils


ARGO_WORKFLOW_LABEL_KEY = "workflows.argoproj.io/workflow"

KFP_COMPONENT_SPEC_ANNOTATION_KEY = "pipelines.kubeflow.org/component_spec"
KFP_RUN_ID_LABEL_KEY = "pipeline/runid"

RUN_CONTEXT_TYPE_NAME = "KfpRun"
KFP_EXECUTION_TYPE_NAME_PREFIX = "components."

METADATA_CONTEXT_ID_LABEL_KEY = "pipelines.kubeflow.org/metadata_context_id"
METADATA_EXECUTION_ID_LABEL_KEY = ("pipelines.kubeflow.org/"
                                   "metadata_execution_id")
METADATA_ARTIFACT_IDS_ANNOTATION_KEY = ("pipelines.kubeflow.org/"
                                        "metadata_artifact_ids")
METADATA_INPUT_ARTIFACT_IDS_ANNOTATION_KEY = ("pipelines.kubeflow.org/"
                                              "metadata_input_artifact_ids")
METADATA_OUTPUT_ARTIFACT_IDS_ANNOTATION_KEY = ("pipelines.kubeflow.org/"
                                               "metadata_output_artifact_ids")
METADATA_WRITTEN_LABEL_KEY = "pipelines.kubeflow.org/metadata_written"

ROK_SNAPSHOT_ARTIFACT_PROPERTIES = {"name": metadata_store_pb2.STRING,
                                    "id": metadata_store_pb2.STRING,
                                    "version": metadata_store_pb2.STRING,
                                    "object": metadata_store_pb2.STRING,
                                    "bucket": metadata_store_pb2.STRING,
                                    "members": metadata_store_pb2.INT,
                                    "URL": metadata_store_pb2.STRING,
                                    "hash": metadata_store_pb2.STRING}
MLMD_EXECUTION_HASH_PROPERTY_KEY = "arrikto.com/execution-hash-key"


# Kubernetes
pod_name = None
pod_namespace = None
pod = None
workflow_name = None
workflow = None

# Rok
rok_client = None

# KFP
run_uuid = None
pipeline_name = None
componend_id = None

# MLMD
mlmd_store = None
execution = None
run_context = None
rok_snapshot_type_id = None
execution_hash = None


def _connect():
    metadata_service_host = os.environ.get(
        'METADATA_GRPC_SERVICE_SERVICE_HOST', 'metadata-grpc-service')
    metadata_service_port = int(os.environ.get(
        'METADATA_GRPC_SERVICE_SERVICE_PORT', 8080))

    mlmd_connection_config = metadata_store_pb2.MetadataStoreClientConfig(
        host=metadata_service_host, port=metadata_service_port)

    for _ in range(100):
        try:
            mlmd_store = metadata_store.MetadataStore(mlmd_connection_config)
            _ = mlmd_store.get_context_types()
            return mlmd_store
        except Exception as e:
            print("Failed to access the Metadata store."
                  " Exception: '{}'".format(str(e)), file=sys.stderr)
            sys.stderr.flush()
            sleep(1)

    raise RuntimeError("Could not connect to the Metadata store.")


def _get_or_create_artifact_type(type_name, properties: dict = None):
    return _get_or_create_entity_type("artifact", type_name, properties)


def _get_or_create_execution_type(type_name, properties: dict = None):
    return _get_or_create_entity_type("execution", type_name, properties)


def _get_or_create_context_type(type_name, properties: dict = None):
    return _get_or_create_entity_type("context", type_name, properties)


def _get_or_create_entity_type(mlmd_entity, type_name,
                               properties: dict = None):
    getter = getattr(_get_mlmd_store(), "get_%s_type" % mlmd_entity)
    putter = getattr(_get_mlmd_store(), "put_%s_type" % mlmd_entity)
    mlmd_entity_type_class = getattr(metadata_store_pb2,
                                     "%sType" % mlmd_entity.title())
    try:
        mlmd_entity_type = getter(type_name=type_name)
        return mlmd_entity_type
    except Exception:
        mlmd_entity_type = mlmd_entity_type_class(name=type_name,
                                                  properties=properties)
        mlmd_entity_type.id = putter(mlmd_entity_type)
        return mlmd_entity_type


def _create_artifact_with_type(uri: str, type_name: str,
                               property_types: dict = None,
                               properties: dict = None,
                               custom_properties: dict = None):
    artifact_type = _get_or_create_artifact_type(type_name=type_name,
                                                 properties=property_types)
    artifact = metadata_store_pb2.Artifact(uri=uri, type_id=artifact_type.id,
                                           properties=properties,
                                           custom_properties=custom_properties)
    artifact.id = _get_mlmd_store().put_artifacts([artifact])[0]
    return artifact


def _create_execution_with_type(type_name: str, property_types: dict = None,
                                properties: dict = None,
                                custom_properties: dict = None):
    execution_type = _get_or_create_execution_type(type_name=type_name,
                                                   properties=property_types)
    execution = metadata_store_pb2.Execution(
        type_id=execution_type.id, properties=properties,
        custom_properties=custom_properties)
    execution.id = _get_mlmd_store().put_executions([execution])[0]
    return execution


def _get_or_create_context_with_type(context_name: str, type_name: str,
                                     property_types: dict = None,
                                     properties: dict = None):
    def get_context_by_name(context_name: str):
        matching_contexts = [context
                             for context in _get_mlmd_store().get_contexts()
                             if context.name == context_name]
        assert len(matching_contexts) <= 1
        return None if len(matching_contexts) == 0 else matching_contexts[0]

    def create_context_with_type(context_name: str, type_name: str,
                                 property_types: dict = None,
                                 properties: dict = None):
        context_type = _get_or_create_context_type(type_name=type_name,
                                                   properties=property_types)
        context = metadata_store_pb2.Context(name=context_name,
                                             type_id=context_type.id,
                                             properties=properties)
        context.id = _get_mlmd_store().put_contexts([context])[0]
        return context

    context = get_context_by_name(context_name)
    if not context:
        context = create_context_with_type(context_name=context_name,
                                           type_name=type_name,
                                           property_types=property_types,
                                           properties=properties)
        # return context

    # # Verifying that the context has the expected type name
    # context_types = store.get_context_types_by_id([context.type_id])
    # assert len(context_types) == 1
    # if context_types[0].name != type_name:
    #     raise RuntimeError("Context '{}' was found, but it has type '{}'"
    #                        " instead of '{}'".format(context_name,
    #                                                  context_types[0].name,
    #                                                  type_name))
    return context


def _get_or_create_run_context():
    run_uuid = _get_run_uuid()
    workflow_name = _get_workflow_name()
    pipeline_name = _get_pipeline_name()
    context_name = "%s.%s" % (re.sub("-", "_", pipeline_name), workflow_name)

    property_types = {"run_id": metadata_store_pb2.STRING,
                      "pipeline_name": metadata_store_pb2.STRING,
                      "workflow_name": metadata_store_pb2.STRING}
    properties = {"run_id": metadata_store_pb2.Value(string_value=run_uuid),
                  "pipeline_name":
                      metadata_store_pb2.Value(string_value=pipeline_name),
                  "workflow_name":
                      metadata_store_pb2.Value(string_value=workflow_name)}

    return _get_or_create_context_with_type(context_name=context_name,
                                            type_name=RUN_CONTEXT_TYPE_NAME,
                                            property_types=property_types,
                                            properties=properties)


def _create_execution_in_run_context():
    run_uuid = _get_run_uuid()
    pipeline_name = _get_pipeline_name()
    component_id = _get_component_id()
    execution_hash = _get_execution_hash()

    property_types = {"run_id": metadata_store_pb2.STRING,
                      "pipeline_name": metadata_store_pb2.STRING,
                      "component_id": metadata_store_pb2.STRING}
    properties = {"run_id": metadata_store_pb2.Value(string_value=run_uuid),
                  "pipeline_name":
                      metadata_store_pb2.Value(string_value=pipeline_name),
                  "component_id":
                      metadata_store_pb2.Value(string_value=component_id)}
    custom_props = {MLMD_EXECUTION_HASH_PROPERTY_KEY:
                    metadata_store_pb2.Value(string_value=execution_hash)}
    execution = _create_execution_with_type(type_name=component_id,
                                            property_types=property_types,
                                            properties=properties,
                                            custom_properties=custom_props)

    run_context = _get_run_context()
    association = metadata_store_pb2.Association(execution_id=execution.id,
                                                 context_id=run_context.id)
    _get_mlmd_store().put_attributions_and_associations([], [association])
    return execution


def _patch_pod(annotation_key, ids):
    execution = _get_execution()
    run_context = _get_run_context()

    pod = _get_latest_pod()
    labels = pod.metadata.labels
    annotations = pod.metadata.annotations

    labels.setdefault(METADATA_EXECUTION_ID_LABEL_KEY, str(execution.id))
    labels.setdefault(METADATA_CONTEXT_ID_LABEL_KEY, str(run_context.id))

    all_ids_str = annotations.get(annotation_key, "[]")
    all_ids = json.loads(all_ids_str)
    all_ids.extend(ids)
    all_ids.sort()
    all_ids_str = json.dumps(all_ids)
    annotations[annotation_key] = all_ids_str

    metadata = {"labels": labels, "annotations": annotations}
    pod_utils.patch_pod(_get_pod_name(), _get_namespace(),
                        {"metadata": metadata})


def _patch_artifact_inputs(ids):
    _patch_pod(METADATA_INPUT_ARTIFACT_IDS_ANNOTATION_KEY, ids)


def _patch_artifact_outputs(ids):
    _patch_pod(METADATA_OUTPUT_ARTIFACT_IDS_ANNOTATION_KEY, ids)


def _get_pod_name():
    global pod_name
    if not pod_name:
        pod_name = pod_utils.get_pod_name()
    return pod_name


def _get_namespace():
    global pod_namespace
    if not pod_namespace:
        pod_namespace = pod_utils.get_namespace()
    return pod_namespace


def _get_latest_pod():
    k8s_client = pod_utils._get_k8s_v1_client()
    return k8s_client.read_namespaced_pod(_get_pod_name(),
                                          _get_namespace())


def _get_pod():
    global pod
    if not pod:
        pod = _get_latest_pod()
    return pod


def _get_workflow_name():
    global workflow_name
    if not workflow_name:
        pod = _get_pod()
        workflow_name = pod.metadata.labels.get(ARGO_WORKFLOW_LABEL_KEY)
    return workflow_name


def _get_workflow():
    global workflow
    if not workflow:
        api_group = "argoproj.io"
        api_version = "v1alpha1"
        co_name = "workflows"
        namespace = _get_namespace()
        workflow_name = _get_workflow_name()
        co_client = pod_utils._get_k8s_custom_objects_client()
        workflow = co_client.get_namespaced_custom_object(api_group,
                                                          api_version,
                                                          namespace, co_name,
                                                          workflow_name)
    return workflow


def _get_rok_client():
    global rok_client
    if not rok_client:
        from rok_gw_client.client import RokClient
        rok_client = RokClient()
    return rok_client


def _get_run_uuid():
    global run_uuid
    if not run_uuid:
        workflow_labels = _get_workflow()["metadata"].get("labels", {})
        run_uuid = workflow_labels.get(pod_utils.KFP_RUN_ID_LABEL_KEY,
                                       _get_workflow_name())
    return run_uuid


def _get_pipeline_name():
    global pipeline_name
    if not pipeline_name:
        workflow_annotations = _get_workflow()["metadata"].get("annotations",
                                                               {})
        pipeline_spec = json.loads(workflow_annotations.get(
            "pipelines.kubeflow.org/pipeline_spec", {}))
        pipeline_name = pipeline_spec.get("name", _get_workflow_name())
    return pipeline_name


def _get_component_id():
    """Get unique component ID.

    Kale steps are KFP SDK Components. This is the way MetadataWriter generates
    unique names for such components.
    """
    pod = _get_pod()
    component_spec_text = pod.metadata.annotations.get(
        KFP_COMPONENT_SPEC_ANNOTATION_KEY, "{}")
    component_spec = json.loads(component_spec_text)
    component_spec_digest = hashlib.sha256(
        component_spec_text.encode()).hexdigest()
    component_name = component_spec.get("name")
    return component_name + "@sha256=" + component_spec_digest


def _get_execution_hash():
    global execution_hash
    if not execution_hash:
        pod = _get_pod()
        execution_hash = pod.metadata.annotations.get(
            MLMD_EXECUTION_HASH_PROPERTY_KEY, utils.random_string(10))
    return execution_hash


def _get_mlmd_store():
    global mlmd_store
    if not mlmd_store:
        mlmd_store = _connect()
    return mlmd_store


def _get_execution():
    global execution
    if not execution:
        execution = _create_execution_in_run_context()
    return execution


def _get_run_context():
    global run_context
    if not run_context:
        run_context = _get_or_create_run_context()
    return run_context


def _get_rok_snapshot_type_id():
    global rok_snapshot_type_id
    if not rok_snapshot_type_id:
        rok_snapshot_type_id = _get_or_create_artifact_type(
            "RokSnapshot", ROK_SNAPSHOT_ARTIFACT_PROPERTIES).id
    return rok_snapshot_type_id


def _link_artifact(artifact, event_type):
    execution = _get_execution()
    artifact_name = artifact.properties["name"].string_value
    step = metadata_store_pb2.Event.Path.Step(key=artifact_name)
    path = metadata_store_pb2.Event.Path(steps=[step])
    event = metadata_store_pb2.Event(execution_id=execution.id,
                                     artifact_id=artifact.id,
                                     type=event_type,
                                     path=path)

    _get_mlmd_store().put_events([event])


def _link_artifact_as_output(artifact):
    _link_artifact(artifact, metadata_store_pb2.Event.OUTPUT)

    run_context = _get_run_context()
    attribution = metadata_store_pb2.Attribution(context_id=run_context.id,
                                                 artifact_id=artifact.id)
    _get_mlmd_store().put_attributions_and_associations([attribution], [])


def _link_artifact_as_input(artifact):
    _link_artifact(artifact, metadata_store_pb2.Event.INPUT)


def _create_rok_artifact_from_task(task):
    rok_client = _get_rok_client()
    result = task["task"]["result"]
    snapshot_id = result["event"]["id"]
    version = result["event"]["version"]
    obj = result["event"]["object"]
    bucket = task["task"]["bucket"]
    artifact_name = task["task"]["action_params"]["params"]["commit_title"]
    task_info = rok_client.version_info(bucket, obj, version)
    members = int(task_info["group_member_count"])
    url = task_info["rok_url"]
    uri = "/rok/buckets/%s/files/%s/versions/%s" % (bucket, obj, version)

    property_types = ROK_SNAPSHOT_ARTIFACT_PROPERTIES

    values = {"name": metadata_store_pb2.Value(string_value=artifact_name),
              "id": metadata_store_pb2.Value(string_value=snapshot_id),
              "version": metadata_store_pb2.Value(string_value=version),
              "object": metadata_store_pb2.Value(string_value=obj),
              "bucket": metadata_store_pb2.Value(string_value=bucket),
              "members": metadata_store_pb2.Value(int_value=members),
              "URL": metadata_store_pb2.Value(string_value=url),
              "hash": metadata_store_pb2.Value(string_value=task_info["hash"])}

    custom_properties = dict()
    for i in range(members):
        member_name = "member_%s" % i
        member_obj = task_info.get("group_%s_object" % member_name)
        member_version = task_info.get("group_%s_version" % member_name)
        if not member_obj or not member_version:
            continue
        member_info = rok_client.version_info(bucket, member_obj,
                                              member_version)
        member_mount_point = metadata_store_pb2.Value(
            string_value=member_info.get("meta_mountpoint"))
        member_url = metadata_store_pb2.Value(
            string_value=member_info.get("rok_url"))
        member_hash = metadata_store_pb2.Value(
            string_value=member_info.get("hash"))
        custom_properties["%s_URL" % member_name] = member_url
        custom_properties["%s_mount_point" % member_name] = member_mount_point
        custom_properties["%s_hash" % member_name] = member_hash

    return _create_artifact_with_type(uri, "RokSnapshot", property_types,
                                      values, custom_properties or None)


def submit_output_rok_artifact(task):
    """Submit a RokSnapshot MLMD Artifact as output.

    Args:
        task: A Rok task as returned from version_register/version_info
    """
    rok_artifact = _create_rok_artifact_from_task(task)
    _link_artifact_as_output(rok_artifact)
    _patch_artifact_outputs([rok_artifact.id])


def _get_output_rok_artifacts(pod_names):
    output_artifact_ids = []
    annotation = METADATA_OUTPUT_ARTIFACT_IDS_ANNOTATION_KEY
    k8s_client = pod_utils._get_k8s_v1_client()
    for name in pod_names:
        pod = k8s_client.read_namespaced_pod(name, _get_namespace())
        ids = json.loads(pod.metadata.annotations.get(annotation, "[]"))
        output_artifact_ids.extend(ids)

    mlmd_store = _get_mlmd_store()
    return [a for a in mlmd_store.get_artifacts_by_id(output_artifact_ids)
            if a.type_id == _get_rok_snapshot_type_id()]


def submit_input_rok_artifacts():
    """Search in workflow.Status.Nodes for step's parents."""
    pod_name = _get_pod_name()
    workflow = _get_workflow()
    workflow_name = _get_workflow_name()
    nodes = workflow["status"]["nodes"]

    # Find all nodes that are direct ancestors
    parents = []
    for name, node in nodes.items():
        if pod_name in node.get("children", []) and name != workflow_name:
            parents.append(name)

    rok_artifacts = _get_output_rok_artifacts(parents)

    # Link artifacts as inputs
    input_ids = []
    for artifact in rok_artifacts:
        input_ids.append(artifact.id)
        _link_artifact_as_input(artifact)
    _patch_artifact_inputs(input_ids)
