import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { resolveMediaSource } from "@t3tools/client-runtime/media-source";
import { getBrowseDirectoryPath } from "@t3tools/client-runtime/state/projects";
import { useCallback, useMemo, useState } from "react";
import { RefreshControl, ScrollView, View } from "react-native";

import { MarkdownContent } from "../../components/MarkdownContent";
import {
  ThreadMarkdownImage,
  ThreadMarkdownImageUnavailable,
} from "../threads/ThreadMarkdownImage";
import type { MarkdownImageRenderer } from "../../native/SelectableMarkdownText";
import { resolveWorkspaceFilePath } from "./filePath";

export function FileMarkdownPreview(props: {
  readonly cwd: string;
  readonly captured?: boolean;
  readonly environmentId: EnvironmentId;
  readonly markdown: string;
  readonly relativePath: string;
  /** Absent for a file opened from a project draft, which has no thread yet. */
  readonly threadId: ThreadId | null;
  readonly onRefresh?: () => Promise<void> | void;
}) {
  const [isPullRefreshing, setIsPullRefreshing] = useState(false);
  const handlePullToRefresh = useCallback(async () => {
    if (!props.onRefresh) {
      return;
    }
    setIsPullRefreshing(true);
    try {
      await props.onRefresh();
    } finally {
      setIsPullRefreshing(false);
    }
  }, [props.onRefresh]);
  const markdownDirectory = useMemo(
    () => getBrowseDirectoryPath(resolveWorkspaceFilePath(props.cwd, props.relativePath)),
    [props.cwd, props.relativePath],
  );
  const renderImage = useCallback<MarkdownImageRenderer>(
    (image) => {
      const media = resolveMediaSource(image.href, {
        threadId: props.threadId ?? undefined,
        workspaceRoot: markdownDirectory,
        imageEmbed: true,
      });
      if (media?.access === "direct") {
        return null;
      }
      if (
        props.captured ||
        media === null ||
        media.kind !== "image" ||
        media.access === "unavailable"
      ) {
        return <ThreadMarkdownImageUnavailable alt={image.alt} />;
      }
      return (
        <ThreadMarkdownImage
          environmentId={props.environmentId}
          resource={media.resource}
          alt={image.alt}
          srcFragment={media.srcFragment}
          onPressPreview={() => undefined}
        />
      );
    },
    [markdownDirectory, props.environmentId, props.threadId, props.captured],
  );

  return (
    <ScrollView
      className="flex-1 bg-sheet"
      contentContainerStyle={{ padding: 18 }}
      refreshControl={
        props.onRefresh ? (
          <RefreshControl
            refreshing={isPullRefreshing}
            onRefresh={() => void handlePullToRefresh()}
          />
        ) : undefined
      }
    >
      <View className="mx-auto w-full max-w-[760px]">
        <MarkdownContent markdown={props.markdown} renderImage={renderImage} />
      </View>
    </ScrollView>
  );
}
