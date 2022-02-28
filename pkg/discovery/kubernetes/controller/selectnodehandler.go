/*
Copyright 2021 Loggie Authors

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
	"fmt"
	"os"

	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/source"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/listers/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/helper"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/loggie-io/loggie/pkg/source/file"
	"github.com/loggie-io/loggie/pkg/util"

	"github.com/pkg/errors"
)

func (c *Controller) handleLogConfigTypeNode(lgc *logconfigv1beta1.LogConfig) error {
	pipRaws, err := buildPipelineFromLogConfig(c.config, lgc, c.sinkLister, c.interceptorLister, c.nodeLabels)
	if err != nil {
		return errors.WithMessage(err, "convert to pipeline config failed")
	}

	pipCopy, err := pipRaws.DeepCopy()
	if err != nil {
		return errors.WithMessage(err, "deep copy pipeline config error")
	}
	pipCopy.SetDefaults()
	if err = pipCopy.Validate(); err != nil {
		return err
	}

	lgcKey := helper.MetaNamespaceKey(lgc.Namespace, lgc.Name)
	if err := c.typeNodeIndex.ValidateAndSetConfig(lgcKey, []pipeline.ConfigRaw{*pipRaws}); err != nil {
		return err
	}

	// check node selector
	if lgc.Spec.Selector.NodeSelector.NodeSelector != nil {
		if !helper.LabelsSubset(lgc.Spec.Selector.NodeSelector.NodeSelector, c.nodeLabels) {
			log.Debug("logConfig %s/%s is not belong to this node", lgc.Namespace, lgc.Name)
			return nil
		}
	}

	if err = c.syncConfigToFile(logconfigv1beta1.SelectorTypeNode); err != nil {
		return errors.WithMessage(err, "failed to sync config to file")
	}
	log.Info("handle logConfig %s/%s addOrUpdate event and sync config file success", lgc.Namespace, lgc.Name)
	return nil
}

func buildPipelineFromLogConfig(config *Config, lgc *logconfigv1beta1.LogConfig,
	sinkLister v1beta1.SinkLister, icpLister v1beta1.InterceptorLister, nodeLabels map[string]string) (*pipeline.ConfigRaw, error) {
	logConf := lgc.DeepCopy()
	srcConfList := make([]fileSource, 0)
	err := cfg.UnpackRaw([]byte(logConf.Spec.Pipeline.Sources), &srcConfList)
	if err != nil {
		return nil, errors.WithMessagef(err, "unpack logConfig %s sources failed", lgc.Namespace)
	}

	fileSources := make([]fileSource, 0)
	for _, sourceConf := range srcConfList {
		fileSource, err := updateSource(config, sourceConf, logConf.Name, nodeLabels)
		if err != nil {
			return nil, err
		}

		fileSources = append(fileSources, fileSource...)
	}

	pipeConfig, err := toPipeConfig(lgc.Namespace, lgc.Name, logConf.Spec.Pipeline, fileSources, sinkLister, icpLister)
	if err != nil {
		return nil, err
	}
	return pipeConfig, nil
}

func updateSource(config *Config, fSrc fileSource, lgcName string, nodeLabels map[string]string) ([]fileSource, error) {
	fileSources := make([]fileSource, 0)
	fileSource := fileSource{}
	if err := util.Clone(fSrc, &fileSource); err != nil {
		return nil, err
	}

	src, err := fileSource.getSource()
	if err != nil {
		return nil, err
	}
	if src.Type != file.Type {
		return nil, errors.New("only source type=file is supported when selector.type=node")
	}

	src.Name = genTypeNodeSourceName(config.NodeName, src.Name)

	injectNodeFields(config, fSrc.MatchFields, src, lgcName, nodeLabels)

	if err := fileSource.setSource(src); err != nil {
		return nil, err
	}

	fileSources = append(fileSources, fileSource)
	return fileSources, nil
}

func genTypeNodeSourceName(nodeName string, sourceName string) string {
	return fmt.Sprintf("%s/%s", nodeName, sourceName)
}

func injectNodeFields(config *Config, match *matchFields, src *source.Config, lgcName string, nodeLabels map[string]string) {
	if src.Fields == nil {
		src.Fields = make(map[string]interface{})
	}

	fields := config.Fields
	if fields.NodeName != "" {
		src.Fields[fields.NodeName] = config.NodeName
	}

	if fields.LogConfig != "" {
		src.Fields[fields.LogConfig] = lgcName
	}

	if match != nil {
		for from, to := range match.Envs {
			envVal := os.Getenv(from)
			if len(envVal) > 0 {
				src.Fields[to] = envVal
			}
		}

		for from, to := range match.Labels {
			if labelVal, exist := nodeLabels[from]; exist && len(labelVal) > 0 {
				src.Fields[to] = labelVal
			}
		}
	}
}
