# How to become a contributor and submit your own code

## Contributor License Agreements

We'd love to accept your sample apps and patches! Before we can take them, we
have to jump a couple of legal hurdles.

Please fill out either the individual or corporate Contributor License Agreement
(CLA).

  * If you are an individual writing original source code and you're sure you
    own the intellectual property, then you'll need to sign an [individual CLA]
    (https://developers.google.com/open-source/cla/individual).
  * If you work for a company that wants to allow you to contribute your work,
    then you'll need to sign a [corporate CLA]
    (https://developers.google.com/open-source/cla/corporate).

Follow either of the two links above to access the appropriate CLA and
instructions for how to sign and return it. Once we receive it, we'll be able to
accept your pull requests.

## Contributing A Patch

1. Submit an issue describing your proposed change to the repo in question.
1. The repo owner will respond to your issue promptly.
1. If your proposed change is accepted, and you haven't already done so, sign a
   Contributor License Agreement (see details above).
1. Fork the desired repo, and create a branch.  Develop and test your code changes on that branch.
1. Ensure that your code adheres to the existing style in the sample to which
   you are contributing. Refer to the
   [Google Cloud Platform Samples Style Guide]
   (https://github.com/GoogleCloudPlatform/Template/wiki/style.html) for the
   recommended coding standards for this organization.
1. Ensure that your code has an appropriate set of unit tests which all pass.
1. Submit a pull request.
1. After the review is done, please squash the commits.

## Squashing commits
Squashing commits rewrites commit history.  All commits that happen on a branch can be consolidated into a single commit.  

### Onetime setup
```
git remote add upstream https://github.com/GoogleCloudPlatform/cloud-bigtable-client.git
```

### Squashing
```
git fetch upstream
git rebase -i upstream/master
```

After which you'll see something like:

```
pick 8d8s9g8 Commit A Comment 
pick 2sdf8sdf Commit B Comment

# Rebase 8d8s9g8..2sdf8sdf onto ba7sdge (2 command(s))
#
# Commands:
# p, pick = use commit
# r, reword = use commit, but edit the commit message
# e, edit = use commit, but stop for amending
# s, squash = use commit, but meld into previous commit
# f, fixup = like "squash", but discard this commit's log message
# x, exec = run command (the rest of the line) using shell
#
# These lines can be re-ordered; they are executed from top to bottom.
#
# If you remove a line here THAT COMMIT WILL BE LOST.
#
# However, if you remove everything, the rebase will be aborted.
#
# Note that empty commits are commented out
```

Replace the word pick with the word squash on all commits except for the first.

```
pick 8d8s9g8 Commit A Comment 
squash 2sdf8sdf Commit B Comment

# Rebase 8d8s9g8..2sdf8sdf onto ba7sdge (2 command(s))
#
# Commands:
# p, pick = use commit
# r, reword = use commit, but edit the commit message
# e, edit = use commit, but stop for amending
# s, squash = use commit, but meld into previous commit
# f, fixup = like "squash", but discard this commit's log message
# x, exec = run command (the rest of the line) using shell
#
# These lines can be re-ordered; they are executed from top to bottom.
#
# If you remove a line here THAT COMMIT WILL BE LOST.
#
# However, if you remove everything, the rebase will be aborted.
#
# Note that empty commits are commented out
```
There might be some conflicts.  If there are conflicts, [resolve them](https://help.github.com/articles/resolving-merge-conflicts-after-a-git-rebase/).  Once you have a single commit, force an update:

```
git push -f
```

View your commit in a browser to confirm that only a single commit remains.
