# HostTopn Integration

## Overview

Monitor the host process

## Setup

### Installation

The HostTopn check is included in the [Datadog Agent][1] package

### Configuration

Edit the `host_topn.d/conf.yaml` file, in the `conf.d/` folder at the root of your [Agent's configuration directory][2]. See the [sample host_topn.d/conf.yaml][3] for all available configuration options:

```
init_config:

instances:
  - N: 20
```