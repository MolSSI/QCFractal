import React, { useEffect, useState } from "react";

interface DelayedLoaderProps {
  isLoading: boolean;
  delay?: number; // milliseconds
  loader: React.ReactNode;
  children: React.ReactNode;
}

export const DelayedLoader: React.FC<DelayedLoaderProps> = ({
  isLoading,
  delay = 250, // milliseconds
  loader,
  children,
}) => {
  const [showLoader, setShowLoader] = useState(false);

  useEffect(() => {
    let timer: ReturnType<typeof setTimeout>;

    if (isLoading) {
      timer = setTimeout(() => {
        setShowLoader(true);
      }, delay);
    } else {
      setShowLoader(false);
    }

    return () => {
      clearTimeout(timer);
    };
  }, [isLoading, delay]);

  if (isLoading && showLoader) {
    return <>{loader}</>;
  }

  if (isLoading) {
    return null; // nothing during the 250ms grace period
  }

  return <>{children}</>;
};
