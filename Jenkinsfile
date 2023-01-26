#!groovy
@Library('lts-basic-pipeline') _

// projName is the directory name for the project on the servers for it's docker/config files
// default values: 
//  registryCredentialsId = "${env.REGISTRY_ID}"
//  registryUri = 'https://registry.lts.harvard.edu'
def endpoints = ['integration']
ltsBasicPipeline.call("jstor-aspace-oai-harvester", "JSTORFORUM", "jstorforum", "25003", endpoints, "lts-jstorforum-alerts")
