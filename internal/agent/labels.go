package agent

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func labelsOf(pod *corev1.Pod) labels.Set { return labels.Set(pod.Labels) }
