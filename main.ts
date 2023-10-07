import $ from "https://deno.land/x/dax@0.35.0/mod.ts"

if(import.meta.main){
  const nodeName = Deno.args[0]
  let status = await getPlanStatus(nodeName)
  if('PLAN_UNDER_PROGRESS' === status){
    console.log("A plan is running.")
    Deno.exit(0);
  }
  const planFile = await generatePlan(nodeName)
  if(!planFile){
    console.log("Generate plan file failed...")
    Deno.exit(1);
  }
  console.log('Plan File: ${planFile}')
  await executePlan(planFile)
  status = await getPlanStatus(nodeName)
  if('PLAN_UNDER_PROGRESS' === status){
    console.log("The plan runs successfully")
    Deno.exit(0);
  }
  throw new Error(status)
}

async function generatePlan(nodeName: string): Promise<string | undefined>{
  const result = await  $`hdfs diskbalancer -plan ${nodeName}`.text()
  const lines = result.split("\n")
  const regex = /\/system\/diskbalancer\/(.*?)$/;
  for(const line of lines){
    const found = line.match(regex)
    if(found){
      return `${found[0]}/${nodeName}.plan.json`
    }
  }
}

async function executePlan(planFile: string): Promise<void> {
  await $`hdfs diskbalancer -execute ${planFile}`
}

async function getPlanStatus(nodeName: string): Promise<string> {
  const result = await $`hdfs diskbalancer -query ${nodeName}`.text();
  const lines = result.split("\n")
  const status = lines[2]
  return status.substring('Result: '.length)
}


