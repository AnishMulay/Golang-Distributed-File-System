# Golang Distributed File System

[![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/AnishMulay/Golang-Distributed-File-System)](https://github.com/AnishMulay/Golang-Distributed-File-System) 
[![GitHub Issues or Pull Requests](https://img.shields.io/github/issues/AnishMulay/Golang-Distributed-File-System)](https://github.com/AnishMulay/Golang-Distributed-File-System/issues)
![GitHub Issues or Pull Requests](https://img.shields.io/github/issues-closed/AnishMulay/Golang-Distributed-File-System)
![GitHub last commit](https://img.shields.io/github/last-commit/AnishMulay/Golang-Distributed-File-System)
![GitHub contributors](https://img.shields.io/github/contributors/AnishMulay/Golang-Distributed-File-System)
![GitHub repo size](https://img.shields.io/github/repo-size/AnishMulay/Golang-Distributed-File-System)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/SE-Spring2025-G2/Recipe-Recommender/blob/main/LICENSE)
[![Tests](https://github.com/AnishMulay/Golang-Distributed-File-System/actions/workflows/main.yml/badge.svg)](https://github.com/AnishMulay/Golang-Distributed-File-System/actions/workflows/main.yml)

## About

This is a content-aware distributed file system written in Go. Initially based on [anthony gg's](https://www.youtube.com/@anthonygg) tutorials, this project extends beyond them, adding new features and improvements.

A distributed file system uses peer-to-peer (P2P) connections to store and manage files across multiple nodes. Instead of relying on a central server, files are replicated across multiple peers, ensuring redundancy and fault tolerance. If one node goes down, other nodes still retain copies, preventing data loss and improving availability.
