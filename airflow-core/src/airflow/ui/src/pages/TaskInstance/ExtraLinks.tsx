/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { Box, Button, Heading, HStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useTaskInstanceServiceGetExtraLinks } from "openapi/queries";

type ExtraLinksProps = {
  readonly refetchInterval: number | false;
};

export const ExtraLinks = ({ refetchInterval }: ExtraLinksProps) => {
  const { t: translate } = useTranslation("dag");
  const { dagId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();

  const { data } = useTaskInstanceServiceGetExtraLinks(
    {
      dagId,
      dagRunId: runId,
      mapIndex: parseInt(mapIndex, 10),
      taskId,
    },
    undefined,
    {
      refetchInterval,
    },
  );

  return data && data.extra_links && data.extra_links.length > 0 ? (
    <Box py={1}>
      <Heading size="sm">{translate("extraLinks")}</Heading>
      <HStack gap={2} py={2}>
        {/* Map over the new array of link objects */}
        {data.extra_links.map((link) =>
          link.url === null ? undefined : (
            <Button asChild colorPalette="brand" key={link.name} variant="surface">
              <a 
                href={link.url} 
                target={link.target || "_blank"}
                rel={link.target === "_blank" || !link.target ? "noopener noreferrer" : undefined}
              >
                {link.name}
              </a>
            </Button>
          ),
        )}
      </HStack>
    </Box>
  ) : undefined;
};