/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	appsv1alpha1 "github.com/boicotaz/taskapp-operator/api/v1alpha1"
)

const backendFinalizer = "apps.taskapp.io/finalizer"

// BackendReconciler reconciles a Backend object
type BackendReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps.taskapp.io,resources=backends,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.taskapp.io,resources=backends/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.taskapp.io,resources=backends/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete

func (r *BackendReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	backend := &appsv1alpha1.Backend{}
	if err := r.Get(ctx, req.NamespacedName, backend); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !backend.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.handleDeletion(ctx, backend)
	}

	if !controllerutil.ContainsFinalizer(backend, backendFinalizer) {
		controllerutil.AddFinalizer(backend, backendFinalizer)
		if err := r.Update(ctx, backend); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if err := r.reconcileDeployment(ctx, backend); err != nil {
		log.Error(err, "failed to reconcile Deployment")
		return ctrl.Result{}, err
	}

	if err := r.reconcileService(ctx, backend); err != nil {
		log.Error(err, "failed to reconcile Service")
		return ctrl.Result{}, err
	}

	if err := r.updateStatus(ctx, backend); err != nil {
		log.Error(err, "failed to update status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *BackendReconciler) handleDeletion(ctx context.Context, backend *appsv1alpha1.Backend) error {
	if !controllerutil.ContainsFinalizer(backend, backendFinalizer) {
		return nil
	}

	if err := r.deleteDeployment(ctx, backend); err != nil {
		return err
	}
	if err := r.deleteService(ctx, backend); err != nil {
		return err
	}

	controllerutil.RemoveFinalizer(backend, backendFinalizer)
	return r.Update(ctx, backend)
}

func (r *BackendReconciler) deleteDeployment(ctx context.Context, backend *appsv1alpha1.Backend) error {
	deploy := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: deploymentName(backend), Namespace: backend.Namespace}, deploy)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return client.IgnoreNotFound(r.Delete(ctx, deploy))
}

func (r *BackendReconciler) deleteService(ctx context.Context, backend *appsv1alpha1.Backend) error {
	svc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: serviceName(backend), Namespace: backend.Namespace}, svc)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return client.IgnoreNotFound(r.Delete(ctx, svc))
}

func (r *BackendReconciler) reconcileDeployment(ctx context.Context, backend *appsv1alpha1.Backend) error {
	desired := r.buildDeployment(backend)
	if err := ctrl.SetControllerReference(backend, desired, r.Scheme); err != nil {
		return err
	}

	existing := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	if equality.Semantic.DeepEqual(existing.Spec.Replicas, desired.Spec.Replicas) &&
		equality.Semantic.DeepEqual(existing.Spec.Template.Spec, desired.Spec.Template.Spec) {
		return nil
	}
	existing.Spec.Replicas = desired.Spec.Replicas
	existing.Spec.Template.Spec = desired.Spec.Template.Spec
	return r.Update(ctx, existing)
}

func (r *BackendReconciler) reconcileService(ctx context.Context, backend *appsv1alpha1.Backend) error {
	desired := r.buildService(backend)
	if err := ctrl.SetControllerReference(backend, desired, r.Scheme); err != nil {
		return err
	}

	existing := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	if equality.Semantic.DeepEqual(existing.Spec.Ports, desired.Spec.Ports) &&
		equality.Semantic.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) {
		return nil
	}
	existing.Spec.Ports = desired.Spec.Ports
	existing.Spec.Selector = desired.Spec.Selector
	return r.Update(ctx, existing)
}

func (r *BackendReconciler) updateStatus(ctx context.Context, backend *appsv1alpha1.Backend) error {
	deploy := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{Name: deploymentName(backend), Namespace: backend.Namespace}, deploy); err != nil {
		return client.IgnoreNotFound(err)
	}

	backend.Status.ReadyReplicas = deploy.Status.ReadyReplicas

	desired := int32(1)
	if backend.Spec.Replicas != nil {
		desired = *backend.Spec.Replicas
	}

	available := metav1.ConditionFalse
	reason := "DeploymentUnavailable"
	message := fmt.Sprintf("%d/%d replicas ready", deploy.Status.ReadyReplicas, desired)
	if deploy.Status.ReadyReplicas >= desired {
		available = metav1.ConditionTrue
		reason = "DeploymentAvailable"
	}

	cond := metav1.Condition{
		Type:               "Available",
		Status:             available,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: backend.Generation,
		LastTransitionTime: metav1.Now(),
	}

	existing := findCondition(backend.Status.Conditions, "Available")
	if existing != nil {
		existing.Status = cond.Status
		existing.Reason = cond.Reason
		existing.Message = cond.Message
		existing.ObservedGeneration = cond.ObservedGeneration
	} else {
		backend.Status.Conditions = append(backend.Status.Conditions, cond)
	}

	return r.Status().Update(ctx, backend)
}

func (r *BackendReconciler) buildDeployment(backend *appsv1alpha1.Backend) *appsv1.Deployment {
	labels := map[string]string{
		"app.kubernetes.io/name":       "backend",
		"app.kubernetes.io/instance":   backend.Name,
		"app.kubernetes.io/managed-by": "taskapp-operator",
	}

	replicas := int32(1)
	if backend.Spec.Replicas != nil {
		replicas = *backend.Spec.Replicas
	}

	probeHandler := corev1.ProbeHandler{
		HTTPGet: &corev1.HTTPGetAction{
			Path: "/ready",
			Port: intstr.FromInt32(8080),
		},
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName(backend),
			Namespace: backend.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "backend",
							Image: fmt.Sprintf("%s:%s", backend.Spec.Image, backend.Spec.Tag),
							Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
							Env: []corev1.EnvVar{
								{Name: "PORT", Value: "8080"},
								{Name: "DB_HOST", Value: "taskapp-database"},
								{Name: "DB_PORT", Value: "5432"},
								{Name: "DB_USER", Value: "taskuser"},
								{Name: "DB_NAME", Value: "taskdb"},
								{
									Name: "DB_PASSWORD",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{Name: backend.Spec.DBSecret},
											Key:                  "POSTGRES_PASSWORD",
										},
									},
								},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler:        probeHandler,
								InitialDelaySeconds: 5,
								PeriodSeconds:       10,
								FailureThreshold:    3,
							},
						},
					},
				},
			},
		},
	}
}

func (r *BackendReconciler) buildService(backend *appsv1alpha1.Backend) *corev1.Service {
	labels := map[string]string{
		"app.kubernetes.io/name":       "backend",
		"app.kubernetes.io/instance":   backend.Name,
		"app.kubernetes.io/managed-by": "taskapp-operator",
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName(backend),
			Namespace: backend.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{
				{
					Port:       80,
					TargetPort: intstr.FromInt32(8080),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Type: corev1.ServiceTypeClusterIP,
		},
	}
}

func deploymentName(b *appsv1alpha1.Backend) string { return b.Name + "-backend" }
func serviceName(b *appsv1alpha1.Backend) string    { return b.Name + "-backend" }

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *BackendReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1alpha1.Backend{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Named("backend").
		Complete(r)
}
