#!/usr/bin/env bash
if [ -f "$HOME/.bashrc" ] ; then
  source $HOME/.bashrc
else
  source /etc/bashrc
fi

"${@}"