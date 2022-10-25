# Contributing to `dbt_metrics`

1. [About this document](#about-this-document)
2. [Proposing a change](#proposing-a-change)
3. [Getting the code](#getting-the-code)
4. [Running `dbt_metrics` in development](#running-dbtmetrics-in-development)
6. [Testing](#testing)
7. [Adding a changelog entry](#adding-a-changelog-entry)
8. [Submitting a Pull Request](#submitting-a-pull-request)

## About this document

This document is a guide intended for folks interested in contributing to `dbt_metrics`. Below, we document the process by which members of the community should create issues and submit pull requests (PRs) in this repository. 

If you're new to contributing to open-source software, we encourage you to read this document from start to finish. If you get stuck, drop us a line in the [dbt Slack](https://community.getdbt.com).

### Signing the CLA

Please note that all contributors to `dbt_metrics` must sign the [Contributor License Agreement](https://docs.getdbt.com/docs/contributor-license-agreements) to have their Pull Request merged into the codebase. If you are unable to sign the CLA, then the `dbt_metrics` maintainers will unfortunately be unable to merge your Pull Request. You are, however, welcome to open issues and comment on existing ones.

## Proposing a change

`dbt_metrics` is Apache 2.0-licensed open source software. It is what it is today because community members like you have opened issues, provided feedback, and contributed to the knowledge loop for the entire community. Whether you are a seasoned open source contributor or a first-time committer, we welcome and encourage you to contribute code, documentation, ideas, or problem statements to this project.

### Defining the problem

If you have an idea for a new feature or if you've discovered a bug in `dbt_metrics`, the first step is to open an issue. Please check the list of [open issues](https://github.com/dbt-labs/dbt_metrics/issues) before creating a new one. If you find a relevant issue, please add a comment to the open issue instead of creating a new one. **The `dbt_metrics` maintainers are always happy to point contributors in the right direction**, so please err on the side of documenting your idea in a new issue if you are unsure where a problem statement belongs.

> **Note:** All community-contributed Pull Requests _must_ be associated with an open issue. If you submit a Pull Request that does not pertain to an open issue, you will be asked to create an issue describing the problem before the Pull Request can be reviewed.

### Discussing the idea

After you open an issue, a project maintainer will follow up by commenting on your issue (usually within 1-3 days) to explore your idea further and advise on how to implement the suggested changes. In many cases, community members will chime in with their own thoughts on the problem statement. If you as the issue creator are interested in submitting a Pull Request to address the issue, you should indicate this in the body of the issue. The project maintainers are _always_ happy to help contributors with the implementation of fixes and features, so please also indicate if there's anything you're unsure about or could use guidance around in the issue.

### Submitting a change

If an issue is appropriately well scoped and describes a beneficial change to the `dbt_metrics` codebase, then anyone may submit a Pull Request to implement the functionality described in the issue. See the sections below on how to do this.

The maintainers will add a `good first issue` label if an issue is suitable for a first-time contributor. This label often means that the required code change is small, or a net-new addition that does not impact existing functionality. You can see the list of currently open issues on the [Contribute](https://github.com/dbt-labs/dbt_metrics/contribute) page.

Here's a good workflow:
- Comment on the open issue, expressing your interest in contributing the required code change
- Outline your planned implementation. If you want help getting started, ask!
- Follow the steps outlined below to develop locally. Once you have opened a PR, one of the `dbt_metrics` maintainers will work with you to review your code.
- Add a test! Tests are crucial for both fixes and new features alike. We want to make sure that code works as intended, and that it avoids any bugs previously encountered. 

In some cases, the right resolution to an open issue might be tangential to the `dbt_metrics` codebase. The right path forward might be a documentation update or a change that can be made in user-space. In other cases, the issue might describe functionality that the maintainers are unwilling or unable to incorporate into the codebase. When it is determined that an open issue describes functionality that will not translate to a code change in the `dbt_metrics` repository, the issue will be tagged with the `wontfix` label (see below) and closed.

### Using issue labels

The `dbt_metrics` maintainers use labels to categorize open issues.

| tag | description |
| --- | ----------- |
| [triage](https://github.com/dbt-labs/dbt_metrics/labels/triage) | This is a new issue which has not yet been reviewed by a maintainer. This label is removed when a maintainer reviews and responds to the issue. |
| [bug](https://github.com/dbt-labs/dbt_metrics/labels/bug) | This issue represents a defect or regression in `dbt_metrics` |
| [enhancement](https://github.com/dbt-labs/dbt_metrics/labels/enhancement) | This issue represents net-new functionality in `dbt_metrics` |
| [good first issue](https://github.com/dbt-labs/dbt_metrics/labels/good%20first%20issue) | This issue does not require deep knowledge of the `dbt_metrics` codebase to implement. This issue is appropriate for a first-time contributor. |
| [help wanted](https://github.com/dbt-labs/dbt_metrics/labels/help%20wanted) / [discussion](https://github.com/dbt-labs/dbt_metrics/labels/discussion) | Conversation around this issue in ongoing, and there isn't yet a clear path forward. Input from community members is most welcome. |
| [duplicate](https://github.com/dbt-labs/dbt_metrics/issues/duplicate) | This issue is functionally identical to another open issue. The `dbt_metrics` maintainers will close this issue and encourage community members to focus conversation on the other one. |
| [stale](https://github.com/dbt-labs/dbt_metrics/labels/stale) | This is an old issue which has not recently been updated. Stale issues will periodically be closed by `dbt_metrics` maintainers, but they can be re-opened if the discussion is restarted. |
| [wontfix](https://github.com/dbt-labs/dbt_metrics/labels/wontfix) | This issue does not require a code change in the `dbt_metrics` repository, or the maintainers are unwilling/unable to merge a Pull Request which implements the behavior described in the issue. |

## Getting the code

### Installing git

You will need `git` in order to download and modify the `dbt_metrics` source code. On macOS, the best way to download git is to just install [Xcode](https://developer.apple.com/support/xcode/).

### External contributors

If you are not a member of the `dbt-labs` GitHub organization, you can contribute to `dbt_metrics` by forking the `dbt_metrics` repository. For a detailed overview on forking, check out the [GitHub docs on forking](https://help.github.com/en/articles/fork-a-repo). In short, you will need to:

1. fork the `dbt_metrics` repository
2. clone your fork locally
3. check out a new branch for your proposed changes
4. push changes to your fork
5. open a pull request against `dbt-labs/dbt_metrics` from your forked repository

### dbt Labs contributors

If you are a member of the `dbt-labs` GitHub organization, you will have push access to the `dbt_metrics` repo. Rather than forking `dbt_metrics` to make your changes, just clone the repository, check out a new branch, and push directly to that branch.

## Running `dbt_metrics` in development

### Installation

`dbt_metrics` is a dbt package, which can be installed into your existing dbt project using the [local package](https://docs.getdbt.com/docs/building-a-dbt-project/package-management#local-packages) functionality. After adding it to your project's `packages.yml` file, run `dbt deps`.

## Testing

When you create a Pull Request (below), integration tests will automatically run. When you add new functionality or change existing functionality, please also add new tests to ensure that the project remains resilient.

## Testing locally

### Initial setup

Postgres offers the easiest way to test most functionality today. To run the Postgres integration tests, you'll have to do one extra step of setting up the test database:

```shell
docker-compose up -d database
```

### Virtual environment

If you are using a shell other than `zsh` or `bash`, you will need to [adjust the `activate` commands](https://docs.python.org/3/library/venv.html) below accordingly.

<details>
  <summary>Platform-specific instructions for venv activation</summary>

| **Platform** | **Shell**       | **Command to activate virtual environment** |
|--------------|-----------------|---------------------------------------------|
| POSIX        | bash/zsh        | `$ source env/bin/activate`                 |
|              | fish            | `$ source env/bin/activate.fish`            |
|              | csh/tcsh        | `$ source env/bin/activate.csh`             |
|              | PowerShell Core | `$ env/bin/Activate.ps1`                    |
| Windows      | cmd.exe         | `C:\> env\Scripts\activate.bat`             |
|              | PowerShell      | `PS C:\> env\Scripts\Activate.ps1`          |

</details>

```shell
python -m venv env
source env/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install --pre -r dev-requirements.txt
source env/bin/activate
```

You can run integration tests "locally" by configuring a `test.env` file with appropriate environment variables.

```
cp test.env.example test.env
$EDITOR test.env
```

WARNING: The `test.env` file you create is `.gitignore`'d, but please be _extra_ careful to never check in credentials or other sensitive information when developing.

### Test commands

There are many options for invoking `pytest` and choosing which tests to execute. See [here](https://docs.pytest.org/usage.html) for the `pytest` documentation. Some common options are included below.

#### Run all the tests
```shell
python3 -m pytest
```

#### Run tests in a module
```shell
python3 -m pytest tests/functional/example/test_example_failing_test.py
```

#### Run tests in a directory
```shell
python3 -m pytest tests/functional
```

## Adding a CHANGELOG Entry

We use [changie](https://changie.dev) to generate `CHANGELOG` entries. **Note:** Do not edit the `CHANGELOG.md` directly. Your modifications will be lost.

Follow the steps to [install `changie`](https://changie.dev/guide/installation/) for your system.

Once changie is installed and your PR is created, simply run `changie new` and changie will walk you through the process of creating a changelog entry.  Commit the file that's created and your changelog entry is complete!

You don't need to worry about which `dbt_metrics` version your change will go into. Just create the changelog entry with `changie`, and open your PR against the `main` branch. All merged changes will be included in the next minor version of `dbt_metrics`. The metrics maintainers _may_ choose to "backport" specific changes in order to patch older minor versions. In that case, a maintainer will take care of that backport after merging your PR, before releasing the new version of `dbt_metrics`.

## Submitting a Pull Request

A `dbt_metrics` maintainer will review your PR. They may suggest code revision for style or clarity, or request that you add unit or integration test(s). These are good things! We believe that, with a little bit of help, anyone can contribute high-quality code.
- First time contributors should note code checks + unit tests require a maintainer to approve.

Once all tests are passing and your PR has been approved, a `dbt_metrics` maintainer will merge your changes into the active development branch. And that's it! Happy developing :tada: