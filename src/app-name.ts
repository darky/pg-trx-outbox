import { randomUUID } from 'node:crypto'

export const appName = randomUUID().substring(0, 4).toLowerCase()
