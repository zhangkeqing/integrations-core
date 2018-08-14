# Solr Check

## Overview

The Solr check tracks the state and performance of a Solr cluster. It collects metrics like number of documents indexed, cache hits and evictions, average request times, average requests per second, and more.

## Setup
### Installation

The Solr check is included in the [Datadog Agent][1] package, so you don't need to install anything else on your Solr nodes.

This check is JMX-based, so you'll need to enable JMX Remote on your Solr servers. Read the [JMX Check documentation][2] for more information on that.

### Configuration

Edit the `solr.d/conf.yaml` file, in the `conf.d/` folder at the root of your [Agent's configuration directory][5]. See the [sample solr.d/conf.yaml][3] for all available configuration options.

See the [JMX Check documentation][2] for a list of configuration options usable by all JMX-based checks. The page also describes how the Agent tags JMX metrics.

[Restart the Agent][4] to start sending Solr metrics to Datadog.


[1]: https://app.datadoghq.com/account/settings#agent
[2]: https://docs.datadoghq.com/integrations/java/
[3]: https://github.com/DataDog/integrations-core/blob/master/solr/datadog_checks/solr/data/conf.yaml.example
[4]: https://docs.datadoghq.com/agent/faq/agent-commands/#start-stop-restart-the-agent
[5]: https://docs.datadoghq.com/agent/faq/agent-configuration-files/#agent-configuration-directory
