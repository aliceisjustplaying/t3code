import { jobIsActive, jobStateLabel, type ThreadJob } from "@t3tools/client-runtime/jobs";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { formatDuration } from "@t3tools/shared/orchestrationTiming";
import { useEffect, useState } from "react";
import { Alert, Pressable, ScrollView, View } from "react-native";
import { AppText as Text } from "../../components/AppText";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";

export function ThreadJobDetails(props: {
  environmentId: EnvironmentId;
  threadId: ThreadId;
  job: ThreadJob;
  onClose: () => void;
}) {
  const { job } = props;
  const stop = useAtomCommand(threadEnvironment.stopJob, "stop job");
  const [now, setNow] = useState(Date.now);
  const [pending, setPending] = useState(false);
  const live = jobIsActive(job);
  useEffect(() => {
    if (!live) return;
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, [live]);
  return (
    <ScrollView
      className="flex-1 bg-screen"
      contentContainerClassName="p-5 gap-4"
      contentInsetAdjustmentBehavior="automatic"
    >
      <Pressable
        accessibilityRole="button"
        onPress={props.onClose}
        className="min-h-11 justify-center"
      >
        <Text>← Back to thread</Text>
      </Pressable>
      <Text className="text-2xl font-semibold">{job.name}</Text>
      <Text>
        {jobStateLabel[job.state]} · {formatDuration((job.endedAt ?? now) - job.startedAt)}
      </Text>
      <Text>{job.sourceTitle ? `Subagent · ${job.sourceTitle}` : "This thread"}</Text>
      <Text>
        {job.exitCode !== null
          ? `Exit ${job.exitCode}`
          : (job.signal ?? (job.state === "lost" ? "Exit not observed" : "Not finished"))}
      </Text>
      {job.state === "running" && (
        <Pressable
          accessibilityRole="button"
          disabled={pending}
          className="min-h-11 justify-center"
          onPress={() =>
            Alert.alert(
              "Stop this job?",
              `Stop “${job.name}”? The thread and other jobs will keep running.`,
              [
                { text: "Keep running", style: "cancel" },
                {
                  text: "Stop job",
                  style: "destructive",
                  onPress: () => {
                    setPending(true);
                    void stop({
                      environmentId: props.environmentId,
                      input: { threadId: props.threadId, turnItemId: job.turnItemId },
                    }).then((result) => {
                      setPending(false);
                      if (result._tag !== "Success")
                        Alert.alert(
                          "Could not request job stop",
                          "The owning session may have ended.",
                        );
                    });
                  },
                },
              ],
            )
          }
        >
          <Text className="text-red-500">Stop job</Text>
        </Pressable>
      )}
      <Text className="text-xs text-foreground-muted">{job.cwd}</Text>
      <Text>Output · last 16,000 characters</Text>
      <View className="rounded-lg bg-surface p-4">
        <Text selectable className="font-mono text-xs">
          $ {job.command}
          {"\n\n"}
          {job.output || "(no output yet)"}
        </Text>
      </View>
    </ScrollView>
  );
}
