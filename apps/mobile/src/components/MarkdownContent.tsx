import { useMemo } from "react";
import { Text as NativeText } from "react-native";
import {
  Markdown,
  type CustomRenderers,
  type NodeStyleOverrides,
  type PartialMarkdownTheme,
} from "react-native-nitro-markdown";
import { tryOpenExternalUrl } from "../lib/openExternalUrl";
import { useFontFamily } from "../lib/useFontFamily";
import {
  resolveMarkdownFontSizes,
  resolveNativeMarkdownTypography,
} from "../lib/appearancePreferences";
import { useUniwindTheme } from "../lib/useUniwindTheme";
import { useAppearancePreferences } from "../features/settings/appearance/AppearancePreferencesProvider";
import {
  hasNativeSelectableMarkdownText,
  SelectableMarkdownText,
  type MarkdownImageRenderer,
  type NativeMarkdownTextStyle,
} from "../native/SelectableMarkdownText";

interface MarkdownPreviewStyles {
  readonly theme: PartialMarkdownTheme;
  readonly styles: NodeStyleOverrides;
  readonly renderers: CustomRenderers;
  readonly nativeTextStyle: NativeMarkdownTextStyle;
}

function useMarkdownPreviewStyles(renderImage?: MarkdownImageRenderer): MarkdownPreviewStyles {
  const { appearance } = useAppearancePreferences();
  const markdownFontSizes = useMemo(
    () => resolveMarkdownFontSizes(appearance.baseFontSize),
    [appearance.baseFontSize],
  );
  const nativeMarkdownTypography = useMemo(
    () => resolveNativeMarkdownTypography(appearance.baseFontSize),
    [appearance.baseFontSize],
  );
  const theme = useUniwindTheme();
  const body = theme["--color-md-body"];
  const strong = theme["--color-md-strong"];
  const link = theme["--color-md-link"];
  const blockquoteBorder = theme["--color-md-blockquote-border"];
  const blockquoteBackground = theme["--color-md-blockquote-bg"];
  const codeBackground = theme["--color-md-code-bg"];
  const codeText = theme["--color-md-code-text"];
  const horizontalRule = theme["--color-md-hr"];
  const regularFontFamily = useFontFamily("regular");
  const mediumFontFamily = useFontFamily("medium");
  const boldFontFamily = useFontFamily("bold");

  return useMemo(() => {
    const renderers: CustomRenderers = {
      link: ({ href, children }) => (
        <NativeText
          className="font-t3-medium"
          onPress={() => {
            if (href) {
              void tryOpenExternalUrl(href, "markdown-link");
            }
          }}
          style={{
            color: link,
            textDecorationLine: "none",
          }}
        >
          {children}
        </NativeText>
      ),
      image: ({ node }) =>
        node.href && renderImage
          ? (renderImage({
              href: node.href,
              alt: node.alt ?? null,
              title: node.title ?? null,
            }) ?? undefined)
          : undefined,
    };

    return {
      theme: {
        colors: {
          text: body,
          heading: strong,
          link,
          blockquote: blockquoteBorder,
          border: horizontalRule,
          surface: "transparent",
          surfaceLight: blockquoteBackground,
          accent: link,
          tableBorder: horizontalRule,
          tableHeader: blockquoteBackground,
          tableHeaderText: strong,
          tableRowOdd: blockquoteBackground,
          tableRowEven: "transparent",
          code: codeText,
          codeBackground,
        },
      },
      styles: {
        text: {
          color: body,
          fontFamily: regularFontFamily,
          fontSize: markdownFontSizes.m,
          lineHeight: markdownFontSizes.bodyLineHeight,
        },
        heading: {
          color: strong,
          fontFamily: boldFontFamily,
        },
        strong: {
          color: strong,
          fontFamily: boldFontFamily,
        },
        link: {
          color: link,
          fontFamily: mediumFontFamily,
        },
        blockquote: {
          backgroundColor: blockquoteBackground,
          borderLeftColor: blockquoteBorder,
          borderLeftWidth: 3,
          paddingLeft: 12,
        },
        code: {
          backgroundColor: codeBackground,
          color: codeText,
          fontFamily: "ui-monospace",
        },
        codeBlock: {
          backgroundColor: codeBackground,
          borderRadius: 12,
          color: codeText,
          fontFamily: "ui-monospace",
          padding: 12,
        },
        hr: {
          backgroundColor: horizontalRule,
        },
      },
      renderers,
      nativeTextStyle: {
        color: body,
        strongColor: strong,
        mutedColor: body,
        linkColor: link,
        inlineCodeColor: codeText,
        codeColor: codeText,
        codeBackgroundColor: codeBackground,
        codeBlockBackgroundColor: codeBackground,
        fileTextColor: codeText,
        skillTextColor: codeText,
        quoteMarkerColor: blockquoteBorder,
        dividerColor: horizontalRule,
        fontSize: nativeMarkdownTypography.fontSize,
        lineHeight: nativeMarkdownTypography.lineHeight,
        headingFontSizes: nativeMarkdownTypography.headingFontSizes,
        fontFamily: regularFontFamily,
        headingFontFamily: boldFontFamily,
        boldFontFamily,
      },
    };
  }, [
    blockquoteBackground,
    blockquoteBorder,
    body,
    codeBackground,
    codeText,
    horizontalRule,
    link,
    markdownFontSizes,
    mediumFontFamily,
    nativeMarkdownTypography,
    regularFontFamily,
    renderImage,
    strong,
    boldFontFamily,
  ]);
}

function onLinkPress(href: string) {
  void tryOpenExternalUrl(href, "markdown-link");
}

/** The app's themed, selectable Markdown body, without a scroll container. */
export function MarkdownContent(props: {
  readonly markdown: string;
  readonly renderImage?: MarkdownImageRenderer;
}) {
  const styles = useMarkdownPreviewStyles(props.renderImage);
  return hasNativeSelectableMarkdownText() ? (
    <SelectableMarkdownText
      markdown={props.markdown}
      onLinkPress={onLinkPress}
      renderImage={props.renderImage}
      textStyle={styles.nativeTextStyle}
    />
  ) : (
    <Markdown
      options={{ gfm: true }}
      renderers={styles.renderers}
      styles={styles.styles}
      theme={styles.theme}
    >
      {props.markdown}
    </Markdown>
  );
}
