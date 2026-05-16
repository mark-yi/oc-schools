import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  async rewrites() {
    return [
      {
        source: "/ingest/static/:path*",
        destination: "https://us-assets.i.posthog.com/static/:path*"
      },
      {
        source: "/ingest/array/:path*",
        destination: "https://us-assets.i.posthog.com/array/:path*"
      },
      {
        source: "/ingest/:path*",
        destination: "https://us.i.posthog.com/:path*"
      }
    ];
  },
  skipTrailingSlashRedirect: true,
  turbopack: {
    root: process.cwd()
  },
  webpack: (config, { webpack }) => {
    config.plugins.push(
      new webpack.IgnorePlugin({
        resourceRegExp: /^@chroma-core\/default-embed$/
      })
    );
    config.ignoreWarnings = [
      ...(config.ignoreWarnings ?? []),
      {
        module: /chromadb/,
        message: /Critical dependency: the request of a dependency is an expression/
      },
      {
        module: /chromadb/,
        message: /Can't resolve '@chroma-core\/default-embed'/
      }
    ];
    return config;
  }
};

export default nextConfig;
