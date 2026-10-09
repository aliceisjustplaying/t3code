import { useState } from "react";
import { createUseThreadJobs } from "@t3tools/client-runtime/use-thread-jobs";
import { orchestrationEnvironment } from "../../state/orchestration";
import { useEnvironmentQuery } from "../../state/query";

export const useThreadJobs = createUseThreadJobs(useState, function useJobsPage(target) {
  return useEnvironmentQuery(target === null ? null : orchestrationEnvironment.jobsPage(target));
});
