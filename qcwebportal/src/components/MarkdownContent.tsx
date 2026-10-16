import React from "react";
import ReactMarkdown from "react-markdown";
import remarkBreaks from "remark-breaks";
import rehypeRaw from "rehype-raw";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function remarkDisableIndentedCode(this: any) {
  const exts: unknown[] = this.data("micromarkExtensions") ?? [];
  this.data("micromarkExtensions", [...exts, { disable: { null: ["codeIndented"] } }]);
}

const remarkPlugins = [remarkBreaks, remarkDisableIndentedCode];
const rehypePlugins = [rehypeRaw];

const MarkdownContent: React.FC<{ children: string }> = ({ children }) => (
  <ReactMarkdown remarkPlugins={remarkPlugins} rehypePlugins={rehypePlugins}>
    {children}
  </ReactMarkdown>
);

export default MarkdownContent;
