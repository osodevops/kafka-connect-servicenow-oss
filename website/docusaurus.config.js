// @ts-check
import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'ServiceNow Kafka Connector',
  tagline: 'Open-source ServiceNow connectors for Apache Kafka: Table API source and sink, Kafka Connect',
  favicon: 'img/oso-favicon.svg',
  url: process.env.DOCS_URL || 'https://servicenowkafkaconnector.com',
  baseUrl: process.env.DOCS_BASE_URL || '/',

  onBrokenLinks: 'throw',
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  themes: [
    [
      '@easyops-cn/docusaurus-search-local',
      /** @type {import('@easyops-cn/docusaurus-search-local').PluginOptions} */
      ({
        hashed: true,
        docsRouteBasePath: '/',
        indexDocs: true,
        indexBlog: false,
        indexPages: true,
        language: ['en'],
        highlightSearchTermsOnTargetPage: true,
        searchResultLimits: 10,
        explicitSearchResultPath: true,
      }),
    ],
  ],

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl: 'https://github.com/osodevops/kafka-connect-servicenow-oss/tree/main/website/',
          routeBasePath: '/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
        sitemap: {
          changefreq: 'weekly',
          priority: 0.5,
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'ServiceNow Kafka Connector',
        logo: {
          alt: 'OSO Logo',
          src: 'img/oso-logo.svg',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'docsSidebar',
            position: 'left',
            label: 'Docs',
          },
          {
            type: 'search',
            position: 'right',
          },
          {
            href: 'https://github.com/osodevops/kafka-connect-servicenow-oss',
            label: 'GitHub',
            position: 'right',
          },
          {
            href: 'https://www.oso.sh',
            label: 'OSO',
            position: 'right',
          },
          {
            to: '/enterprise-support',
            label: 'Enterprise Support',
            position: 'right',
          },
          {
            href: 'https://oso.sh/contact/',
            label: 'Contact',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {label: 'Getting Started', to: '/getting-started'},
              {label: 'Source Connector', to: '/connectors/source'},
              {label: 'Sink Connector', to: '/connectors/sink'},
              {label: 'Migrating from Confluent', to: '/migration/confluent'},
              {label: 'Enterprise Support', to: '/enterprise-support'},
            ],
          },
          {
            title: 'Community',
            items: [
              {label: 'GitHub', href: 'https://github.com/osodevops/kafka-connect-servicenow-oss'},
              {label: 'Issues', href: 'https://github.com/osodevops/kafka-connect-servicenow-oss/issues'},
              {label: 'Discussions', href: 'https://github.com/osodevops/kafka-connect-servicenow-oss/discussions'},
            ],
          },
          {
            title: 'OSO',
            items: [
              {label: 'oso.sh', href: 'https://www.oso.sh'},
              {label: 'Enterprise Support', to: '/enterprise-support'},
              {label: 'Salesforce Kafka Connector', href: 'https://salesforcekafkaconnector.com'},
              {label: 'Kafka Backup', href: 'https://kafkabackup.com'},
              {label: 'Contact', href: 'https://oso.sh/contact/'},
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} OSO. Built with Docusaurus.<br/>
          <small>This is an independent open-source project and is not affiliated with,
          endorsed, or sponsored by ServiceNow, Inc. or the Apache Software Foundation.
          ServiceNow is a trademark of ServiceNow, Inc. Apache, Apache Kafka, and Kafka are
          trademarks of the Apache Software Foundation.</small>`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['bash', 'yaml', 'json', 'java', 'properties'],
      },
    }),
};

export default config;
