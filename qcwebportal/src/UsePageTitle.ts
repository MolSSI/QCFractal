import { useEffect } from "react";

export function usePageTitle(title: string, restore: boolean = false) {
  useEffect(() => {
    const prev = document.title;
    document.title = `QCArchive: ${title}`;

    return () => {
      if (restore) {
        document.title = prev;
      }
    };
  }, [title, restore]);
}
