import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { useHeadsUpInbox } from "./HeadsUpInbox";
import { StackActions, useNavigation } from "@react-navigation/native";
import { Platform, View } from "react-native";
import { useMemo } from "react";
import { ScreenHeader } from "../../components/ScreenHeader";
import { ScreenHeaderButton } from "../../components/ScreenHeaderButton";
import type { ScreenHeaderAction } from "../../components/ScreenHeader.types";
import { useAdaptiveWorkspaceLayout } from "../layout/AdaptiveWorkspaceLayout";
import type { ThreadInspectorMode } from "./thread-inspector-content-stack";
import { useThreadHeaderOptions } from "./useThreadHeaderOptions";

export function ThreadHeader(
  props: Parameters<typeof useThreadHeaderOptions>[0] & {
    readonly environmentId: EnvironmentId;
    readonly threadId: ThreadId;
    readonly hasThreadCwd: boolean;
    readonly hasWorkspaceRoot: boolean;
    readonly fileInspectorSupported: boolean;
    readonly inspectorMode: ThreadInspectorMode | null;
    readonly onToggleInspector: () => void;
    readonly onOpenGitInspector: () => void;
    readonly onOpenFilesInspector: () => void;
  },
) {
  const navigation = useNavigation();
  const { layout, panes, toggleAuxiliaryPane } = useAdaptiveWorkspaceLayout();
  const { onOpenTerminal, onMergeBack } = props.gitControls;
  const inbox = useHeadsUpInbox(props.environmentId, props.threadId);
  const native = useThreadHeaderOptions({ ...props, headsUpInboxButton: inbox.button });
  const androidHeaderActions = useMemo<ReadonlyArray<ScreenHeaderAction>>(() => {
    const actions: ScreenHeaderAction[] = [];
    if (props.onReturnToThread) {
      actions.push({
        accessibilityLabel: "Return to chat",
        icon: "chevron.left",
        onPress: props.onReturnToThread,
      });
    }
    if (props.hasThreadCwd) {
      const filesVisible = props.inspectorMode === "files" && panes.auxiliaryPaneVisible;
      actions.push({
        accessibilityLabel: filesVisible ? "Close files" : "Open files",
        selected: filesVisible,
        icon: "folder",
        onPress: filesVisible ? toggleAuxiliaryPane : props.onOpenFilesInspector,
      });
    }
    if (props.hasWorkspaceRoot && props.gitControls.canOpenTerminal) {
      actions.push({
        accessibilityLabel: "Open terminal",
        icon: "terminal",
        onPress: () => onOpenTerminal(null),
      });
    }
    actions.push({
      accessibilityLabel: "Open git controls",
      icon: "point.topleft.down.curvedto.point.bottomright.up",
      onPress: props.onOpenGitInspector,
    });
    if (onMergeBack) {
      actions.push({
        accessibilityLabel: "Merge back to source",
        icon: "arrow.triangle.merge",
        onPress: onMergeBack,
      });
    }
    return actions;
  }, [
    props.inspectorMode,
    panes.auxiliaryPaneVisible,
    props.onOpenFilesInspector,
    onOpenTerminal,
    onMergeBack,
    props.onOpenGitInspector,
    toggleAuxiliaryPane,
    props.onReturnToThread,
    props.hasThreadCwd,
    props.hasWorkspaceRoot,
    props.gitControls.canOpenTerminal,
  ]);

  return (
    <>
      <ScreenHeader
        title={props.title}
        subtitle={props.subtitle}
        sidebar={native.sidebar}
        options={native.options}
        optionsVersion={[native.optionsVersion, inbox.revision]}
        trailing={<View style={{ flexDirection: "row", alignItems: "center" }}>
          {Platform.OS === "android" ? inbox.button : null}
          {props.fileInspectorSupported && props.hasThreadCwd ? (
            <ScreenHeaderButton
              accessibilityLabel={
                props.inspectorMode !== null && panes.auxiliaryPaneVisible
                  ? "Hide inspector"
                  : "Show inspector"
              }
              icon="sidebar.right"
              selected={props.inspectorMode !== null && panes.auxiliaryPaneVisible}
              onPress={props.onToggleInspector}
            />
          ) : null}
        </View>}
        onBack={
          layout.usesSplitView
            ? undefined
            : () => {
                // A deep link or cold start has no previous route; Home is the way out.
                // Read the history at press time: it changes without re-rendering this screen.
                if (navigation.canGoBack()) navigation.goBack();
                else navigation.dispatch(StackActions.replace("Home"));
              }
        }
        actions={androidHeaderActions}
        menus={props.gitControls.jobsMenu ? [props.gitControls.jobsMenu] : []}
        hideBottomBorder
      />
      {native.fallback}
      {inbox.modal}
    </>
  );
}
