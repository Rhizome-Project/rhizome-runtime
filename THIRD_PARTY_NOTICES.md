# Third-Party Notices

Rhizome Runtime is licensed under the GNU Affero General Public License v3.0. The following components retain their own licenses and copyright notices.

## Vendored browser asset

`internal/server/assets/force-graph.min.js` is an unmodified copy of the browser bundle published as [`force-graph@1.51.2`](https://www.npmjs.com/package/force-graph/v/1.51.2).

- Upstream source: <https://github.com/vasturiano/force-graph/tree/v1.51.2>
- Source commit: `93727e8efed71563d102dc0fd5a78d5bfbadb09a`
- npm tarball: <https://registry.npmjs.org/force-graph/-/force-graph-1.51.2.tgz>
- npm tarball integrity: `sha512-zZNdMqx8qIQGurgnbgYIUsdXxSfvhfRSIdncsKGv/twUOZpwCsk9hPHmdjdcme1+epATgb41G0rkIGHJ0Wydng==`
- Vendored file SHA-256: `c1f2608b89c779070502d86591ccd78ae132af74dcd97aa2bbea829b03fd4ebb`
- Primary license: MIT, Copyright (c) 2018 Vasco Asturiano

The primary license is reproduced in [`LICENSES/force-graph-1.51.2.txt`](LICENSES/force-graph-1.51.2.txt). The complete notices for the bundle's production dependency closure are reproduced in [`LICENSES/force-graph-1.51.2-bundled-dependencies.txt`](LICENSES/force-graph-1.51.2-bundled-dependencies.txt).

The exact production dependencies resolved by the upstream `v1.51.2` lockfile are:

| Package | Version | License |
| --- | --- | --- |
| `@tweenjs/tween.js` | 25.0.0 | MIT |
| `accessor-fn` | 1.5.3 | MIT |
| `bezier-js` | 6.1.4 | MIT |
| `canvas-color-tracker` | 1.3.2 | MIT |
| `d3-binarytree` | 1.0.2 | MIT |
| `d3-force-3d` | 3.0.6 | MIT |
| `d3-octree` | 1.1.0 | MIT |
| `float-tooltip` | 1.7.5 | MIT |
| `index-array-by` | 1.4.2 | MIT |
| `kapsule` | 1.16.3 | MIT |
| `lodash-es` | 4.17.23 | MIT |
| `preact` | 10.29.0 | MIT |
| `tinycolor2` | 1.6.0 | MIT |
| `d3-array` | 3.2.4 | ISC |
| `d3-color` | 3.1.0 | ISC |
| `d3-dispatch` | 3.0.1 | ISC |
| `d3-drag` | 3.0.0 | ISC |
| `d3-format` | 3.1.2 | ISC |
| `d3-interpolate` | 3.0.1 | ISC |
| `d3-quadtree` | 3.0.1 | ISC |
| `d3-scale` | 4.0.2 | ISC |
| `d3-scale-chromatic` | 3.1.0 | ISC; ColorBrewer notice |
| `d3-selection` | 3.0.0 | ISC |
| `d3-time` | 3.1.0 | ISC |
| `d3-time-format` | 4.1.0 | ISC |
| `d3-timer` | 3.0.1 | ISC |
| `d3-transition` | 3.0.1 | ISC |
| `d3-zoom` | 3.0.0 | ISC |
| `internmap` | 2.0.3 | ISC |
| `d3-ease` | 3.0.1 | BSD-3-Clause |

The checksum recorded in [`THIRD_PARTY_ASSETS.sha256`](THIRD_PARTY_ASSETS.sha256) is the publication gate for this vendored asset.

## Go dependencies

Go dependencies are fetched from the versions and checksums pinned by the two `go.mod` and `go.sum` pairs; their source is not vendored in this repository. Binary distributions must include license material and an SBOM generated from the dependency closure actually compiled for each target platform.

`github.com/mattn/go-localereader@v0.0.1` declares the MIT license in its tagged README but omits a standalone license file from that module release. Its declaration and full upstream MIT text are preserved in [`LICENSES/go-localereader-v0.0.1-MIT.txt`](LICENSES/go-localereader-v0.0.1-MIT.txt).
