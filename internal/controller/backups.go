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
	"encoding/json"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	api "github.com/sindef/mspsql/api/v1alpha1"
)

const defaultWeeklyBackupSchedule = "30 6 * * 0"

type backupSchedule struct {
	backupType string
	expression string
}

func (r *MultiSitePostgresReconciler) reconcileBackupSchedules(ctx context.Context,
	instance *api.MultiSitePostgres, now time.Time, canRun bool,
) (time.Duration, error) {
	if instance.Spec.Backup == nil {
		instance.Status.BackupSchedules = nil
		return 0, nil
	}
	schedules := configuredBackupSchedules(instance.Spec.Backup, r.DefaultBackupSchedule)
	statuses := make([]api.BackupScheduleStatus, 0, len(schedules))
	nextReconcile := time.Duration(0)
	for _, configured := range schedules {
		schedule, err := parseBackupSchedule(configured.expression, instance.Spec.Backup.Schedules.Timezone)
		if err != nil {
			return 0, err
		}
		status := previousBackupScheduleStatus(instance.Status.BackupSchedules, configured.backupType)
		status.Type = configured.backupType
		if status.NextScheduledAt == nil {
			next := metav1.NewTime(schedule.Next(now))
			status.NextScheduledAt = &next
		} else if !status.NextScheduledAt.After(now) {
			if !canRun {
				statuses = append(statuses, status)
				nextReconcile = minimumPositiveDuration(nextReconcile, 30*time.Second)
				continue
			}
			due := status.NextScheduledAt.Time
			next := schedule.Next(due)
			for !next.After(now) {
				due = next
				next = schedule.Next(next)
			}
			if err := r.reconcileBackupDirective(ctx, instance, configured.backupType, due); err != nil {
				return 0, err
			}
			lastScheduled := metav1.NewTime(due)
			nextScheduled := metav1.NewTime(next)
			status.LastScheduledAt = &lastScheduled
			status.NextScheduledAt = &nextScheduled
		}
		statuses = append(statuses, status)
		nextReconcile = minimumPositiveDuration(nextReconcile, status.NextScheduledAt.Sub(now))
	}
	instance.Status.BackupSchedules = statuses
	return nextReconcile, nil
}

func configuredBackupSchedules(backup *api.BackupSpec, defaultSchedule string) []backupSchedule {
	var schedules []backupSchedule
	if backup.Schedules.Full != "" {
		schedules = append(schedules, backupSchedule{backupType: "full", expression: backup.Schedules.Full})
	}
	if backup.Schedules.Differential != "" {
		schedules = append(schedules, backupSchedule{
			backupType: "diff", expression: backup.Schedules.Differential,
		})
	}
	if backup.Schedules.Incremental != "" {
		schedules = append(schedules, backupSchedule{
			backupType: "incr", expression: backup.Schedules.Incremental,
		})
	}
	if len(schedules) == 0 {
		if defaultSchedule == "" {
			defaultSchedule = defaultWeeklyBackupSchedule
		}
		schedules = append(schedules, backupSchedule{backupType: "full", expression: defaultSchedule})
	}
	return schedules
}

func parseBackupSchedule(expression, timezone string) (cron.Schedule, error) {
	if timezone == "" {
		timezone = "UTC"
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return nil, fmt.Errorf("load backup timezone %q: %w", timezone, err)
	}
	schedule, err := cron.ParseStandard("CRON_TZ=" + timezone + " " + expression)
	if err != nil {
		return nil, fmt.Errorf("parse backup schedule %q: %w", expression, err)
	}
	return schedule, nil
}

func previousBackupScheduleStatus(statuses []api.BackupScheduleStatus, backupType string,
) api.BackupScheduleStatus {
	for _, status := range statuses {
		if status.Type == backupType {
			return status
		}
	}
	return api.BackupScheduleStatus{}
}

func (r *MultiSitePostgresReconciler) reconcileBackupDirective(ctx context.Context,
	instance *api.MultiSitePostgres, backupType string, scheduledAt time.Time,
) error {
	name := fmt.Sprintf("mspsql-backup-%s-%d", backupType, scheduledAt.Unix())
	key := client.ObjectKey{Namespace: instance.Namespace, Name: name}
	spec, err := json.Marshal(map[string]string{
		"backupType": backupType, "scheduledAt": scheduledAt.UTC().Format(time.RFC3339),
	})
	if err != nil {
		return err
	}
	operationUID := fmt.Sprintf("%s-backup-%s-%d", instance.UID, backupType, scheduledAt.Unix())
	data := map[string]string{
		"type": "Backup", "instanceRef": instance.Name, "deleting": "false",
		"operationUID": operationUID, "spec.json": string(spec),
	}
	var configMap corev1.ConfigMap
	if err := r.Get(ctx, key, &configMap); apierrors.IsNotFound(err) {
		configMap = corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: instance.Namespace, Name: name,
				Labels: map[string]string{
					"multisite-postgres.dev/directive":    "Backup",
					"multisite-postgres.dev/instance-ref": instance.Name,
					"multisite-postgres.dev/instance-uid": string(instance.UID),
				},
			},
			Data: data,
		}
		if err := controllerutil.SetControllerReference(instance, &configMap, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, &configMap)
	} else if err != nil {
		return err
	}
	return nil
}

func minimumPositiveDuration(current, candidate time.Duration) time.Duration {
	if candidate < time.Second {
		candidate = time.Second
	}
	if current == 0 || candidate < current {
		return candidate
	}
	return current
}
